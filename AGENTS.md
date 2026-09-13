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
