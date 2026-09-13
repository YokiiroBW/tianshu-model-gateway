# 模型运行网关

原生协议、请求转发、线路与用量。

当前为 V2 开发准备骨架，未实现业务服务。旧代码来源见主工作区 workspace.json。

本项目有独立 Git；协调检出不供并发写入，任务在主工作区 worktrees 中进行。工作目录上下文见 .runtime/workspace-context.json，或回到主工作区 docs/development/CURRENT.md。

TS-040 已添加[复用审查与验证边界](docs/protocol-reuse-review.md)和[交接](docs/handoffs/TS-040.md)。没有 `src` 数据入口；TS-041 依赖 TS-001 合同与本次审查。

## 离线验证

Python 3.12+。目标实验只用标准库；旧源码核对直接导入只读参考项目，使用其现有 `cryptography==50.0.1`，无 API Key、网络请求或服务安装。当前机器已核定 Python 3.12.14、cryptography 50.0.1，无需安装依赖。通用命令：

```text
python -B -m unittest discover -s tests -p test_protocol_lab.py -v
python -B -m unittest discover -s tests -v
git diff --check
```

本机 Python 不在 PATH，PowerShell 从本任务根目录执行：

```powershell
& 'C:/Users/Administrator/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe' -B -m unittest discover -s tests -v
```

离开分配 worktree 后，运行旧源码核对前显式设置 `LEGACY_ROUTING_ROOT` 为相同只读参考检出路径。缺参考目录/依赖会失败，不跳过后报全绿。源文件 SHA-256 存在 `tests/fixtures/legacy-source.json`；不要自动接受变化。

`tests/protocol_lab.py` 是可抛弃的规则实验，`tests/fixtures/native.json` 是人为构造的请求及流式片段，`test_legacy_characterization.py` 执行真实旧规划器。绿色测试同时表示“旧行为得到复现”和“实验符合拟定规则”，不表示旧实现符合 V2。流片段不是完整上游响应录制，虚构模型不证明任何接入商支持这些字段组合。
