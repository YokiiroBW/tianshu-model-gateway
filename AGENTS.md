# 项目开发约定

遵循天枢主工作区的 V2 总稿、当前任务卡及已发布的 contracts 版本。任务 worktree 的 .runtime/workspace-context.json 记录主工作区与明确基线。

只在分配的目录内实施，不改其他项目或共享合同；公共入口、依赖锁和迁移主线单人负责。禁止把未配置服务显示成成功。

TS-041 已建立 `src/tianshu_gateway` 实际服务，模型配置唯一发布者仍是平台；不能把 tests 内配置来源替身作为产品服务。唯一合同为主工作区 text-dialogue/v1 1.0.0，修改共享 schema 需交协调者。首轮只有 Chat Completions，不添加有损跨协议转换或默认回退。

实际命令（Windows；Linux 使用 `.venv/bin/`）：

- `uv sync --locked --extra dev --python <Python-3.12+-executable>`：本地安装；清单/锁只由当前任务负责人更新。
- `.venv/Scripts/ruff.exe check src tests/gateway_fixtures.py tests/test_gateway_http.py tests/test_gateway_boundaries.py`：本切片静态检查。
- `.venv/Scripts/ruff.exe format --check src tests/gateway_fixtures.py tests/test_gateway_http.py tests/test_gateway_boundaries.py`：本切片格式检查。
- `.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_gateway*.py' -v`：网关范围检查，包含真实本地 HTTP / CLI 子进程；全部隔离夹具。验证结束关闭服务。
- `.venv/Scripts/python.exe -B -m tianshu_gateway --help`：命令说明；实际启动要求显式部署输入，见 README / docs/gateway-runtime.md。

TS-040 历史离线实验命令：

- `python -B -m unittest discover -s tests -p test_protocol_lab.py -v`：Python 3.12+ 标准库目标夹具。
- `python -B -m unittest discover -s tests -p test_legacy_characterization.py -v`：只读旧源码行为核对，需旧仓库依赖 cryptography==50.0.1；位置从 `.runtime/workspace-context.json` 或 `LEGACY_ROUTING_ROOT` 读取。
- `git diff --check`：差异格式检查。

使用 `-B` 防止在只读 references 生成字节码；不得复制旧源码进入本项目。完整命令和本机解释器见 README。旧源码哈希改变必须重审，不自动更新基线。普通网关变更不运行无关旧仓库/全工作区测试。验证需区分组件、HTTP 替身联合、真实平台/模型接入与生产；SQLite 通过不能冒充 PostgreSQL 通过。

本地隔离开发和提交用于审查；不自动推送、部署或操作真实设备。交付短记录 docs/handoffs/<任务编号>.md，含实际变更、验证、风险及下一步。

TS-042：根 `contracts/model-protocol/v1` 1.0.0 已发布（manifest LF SHA256 `52711a71de56dbceebd1d5d96b2baf59a2d9551168029972d59111480f815141`）；`src/tianshu_gateway/native.py` 与接线后的 `/v1/responses` 消费该包。默认 native 关闭，只有部署显式 `native_enabled` 且提供已发布 native 合同目录时才注册路由，否则该路径仍返回 501。不得用候选 schema、Chat 配置或 Chat 授权开启 Responses；native 账本键空间含 contract/principal/caller/namespace，与旧 Chat 版本及撤销链互不继承。内部验证命令 `.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_responses*.py' -v`；ruff check/format --check 覆盖 src 与三个 test_responses 文件（含 `tests/test_responses_native.py`）。保持依赖锁与公开 Chat 配置路径原样。公开路由的联合验收仍需平台生产者适配与协调者确认，本卡不代替该验收。

TS-103：`src/tianshu_gateway/observability/` 是独立观察适配器，只消费别处已形成的事实，不拥有业务规则；依赖方向为“业务模块导入 observability，observability 不导入任何业务模块”，由 `tests/test_observability.py::EntryAssemblyTests` 以源码扫描固定。冻结 wire 仍是主工作区 `contracts/diagnostics/v1` 1.0.0（`development_frozen_pending_joint_acceptance`），本卡只读、不改根 contracts，也不改 `pyproject.toml` / `uv.lock`。`GET /health/live` 公开只读；`GET /health/ready` 需要独立 `TIANSHU_DIAGNOSTICS_TOKEN`，只读、不写日志、不占业务容量、不建回执行，非必要依赖一律 `not_verified`，未配置服务绝不显示成 ready。日志汇不可用时**新**业务以 503 拒绝而不是先转发再补记；队列/容量满只丢新记录并显式降级，不删除、不截断、不覆盖已存文件。本切片命令：

- `.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_observability.py' -v`：事件目录、封闭记录、关联、落盘先于副作用、有界汇、恢复与装配。
- `.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_health.py' -v`：存活/就绪语义、凭据独立、只读性与不泄漏。
- `.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_container_runtime.py' -v`：Dockerfile 文本性质、健康检查脚本对真实本地 TLS 服务的行为、示例部署文档结构。
- `.venv/Scripts/python.exe -B -m tianshu_gateway log-recovery-check --settings <部署 JSON>`：唯一能证明日志恢复的维护动作（0 成功 / 1 失败 / 2 未配置）。
- ruff check/format --check 覆盖 `src tests/gateway_fixtures.py tests/observability_fixtures.py tests/test_observability.py tests/test_health.py tests/test_container_runtime.py scripts/healthcheck.py`。

**容器镜像从未构建、从未运行**（本机无容器运行时），任何记录都不得写成构建通过；未验证项清单见 `docs/deployment.md` 第 7 节与 `docs/handoffs/TS-103.md`。跨产品依赖（平台生产者接受 `X-Tianshu-Correlation-Id`、集中采集与保留策略）不在本卡内修改，必须如实上报。
