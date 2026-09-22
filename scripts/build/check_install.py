"""Plan or run a new, noneditable gateway install; no test environment is reused."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
import venv

ROOT = Path(__file__).resolve().parents[2]


def executable(env, name="python"):
    return (
        env
        / ("Scripts" if os.name == "nt" else "bin")
        / (name + ".exe" if os.name == "nt" else name)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--scope", required=True)
    args = parser.parse_args()
    scope = Path(args.scope).resolve()
    if scope.exists() or not scope.is_relative_to((ROOT / ".runtime").resolve()):
        parser.error("scope must be a new directory below this checkout's .runtime")
    if sys.version_info[:2] != (3, 12):
        parser.error("use Python 3.12 to match the image's dependency markers")
    plan = {
        "linux_image_executed": False,
        "scope": str(scope),
        "steps": [
            "copy fixed build inputs",
            "install pinned build tools into a new build venv",
            "uv sync --locked dependencies into a separate new runtime venv",
            "build and install a noneditable product wheel",
            "check packages, imports and CLI",
        ],
    }
    if not args.execute:
        print(json.dumps(plan, indent=2))
        return
    scope.mkdir(parents=True)
    source = scope / "source"
    source.mkdir()
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copy2(ROOT / name, source / name)
    shutil.copytree(
        ROOT / "src",
        source / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info"),
    )
    build, runtime = scope / "build", scope / "runtime"
    venv.create(build, with_pip=True)
    bp, rp = executable(build), executable(runtime)
    env = dict(
        os.environ,
        UV_PROJECT_ENVIRONMENT=str(runtime),
        UV_LINK_MODE="copy",
        UV_PYTHON_DOWNLOADS="never",
        UV_NO_CACHE="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    env.pop("PYTHONPATH", None)

    def run(args, cwd=scope):
        subprocess.run([str(x) for x in args], cwd=cwd, env=env, check=True, timeout=600)

    run([bp, "-m", "pip", "install", "--no-deps", "-r", ROOT / "scripts/build/tools.lock"])
    run(
        [
            bp,
            "-m",
            "uv",
            "sync",
            "--locked",
            "--no-dev",
            "--no-editable",
            "--no-install-project",
            "--no-build",
            "--python",
            sys.executable,
        ],
        source,
    )
    run(
        [
            bp,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            scope / "wheels",
            source,
        ]
    )
    wheels = list((scope / "wheels").glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("expected exactly one product wheel")
    run([bp, "-m", "uv", "pip", "install", "--python", rp, "--no-deps", wheels[0]])
    run([bp, "-m", "uv", "pip", "check", "--python", rp])
    run([rp, "-I", "-m", "tianshu_gateway", "--help"])
    run([executable(runtime, "tianshu-model-gateway"), "--help"])
    for command in ("usage-report", "log-recovery-check"):
        run([rp, "-I", "-m", "tianshu_gateway", command, "--help"])
    probe = "import importlib.metadata as m,json,tianshu_gateway as p; print(json.dumps({'packages':{d.metadata['Name'].lower().replace('_','-'):d.version for d in m.distributions()},'module':p.__file__}))"
    evidence = json.loads(subprocess.check_output([str(rp), "-I", "-c", probe], cwd=scope, env=env))
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    expected = {p["name"]: p["version"] for p in lock["package"] if p["name"] != "ruff"}
    if evidence["packages"] != expected or not Path(evidence["module"]).resolve().is_relative_to(
        runtime
    ):
        raise RuntimeError("installed package set or import provenance differs from frozen inputs")
    if (source / "uv.lock").read_bytes() != (ROOT / "uv.lock").read_bytes():
        raise RuntimeError("lock changed during install")
    evidence.update(plan)
    evidence["python"] = sys.version
    evidence["wheel_sha256"] = hashlib.sha256(wheels[0].read_bytes()).hexdigest()
    evidence["input_sha256"] = {
        p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
        for p in ("pyproject.toml", "uv.lock", "Dockerfile", "scripts/build/tools.lock")
    }
    (scope / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
