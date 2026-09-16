# 模型运行网关

原生协议、请求转发、线路与用量。

TS-041 已实现首轮 Python HTTP 数据入口：原生 Chat Completions、平台版本化配置读取、凭据引用、独立持久化路由回执及实际 SSE 转发。当前证据是隔离本地 HTTP 替身；尚未接通实际平台服务或真实模型。旧代码来源见主工作区 workspace.json。

本项目有独立 Git；协调检出不供并发写入，任务在主工作区 worktrees 中进行。工作目录上下文见 .runtime/workspace-context.json，或回到主工作区 docs/development/CURRENT.md。

TS-040 的[复用审查](docs/protocol-reuse-review.md)和[交接](docs/handoffs/TS-040.md)保留为历史基线；其 tests 实验不作为生产代码或 wire 定义。当前消费主工作区 `contracts/text-dialogue/v1` 的 1.0.0（发布提交 `102d347`）与 `contracts/model-protocol/v1` 的 1.0.0（native manifest LF SHA256 `52711a71de56dbceebd1d5d96b2baf59a2d9551168029972d59111480f815141`）。运行时读取所用 schema 并验证 manifest 和文件摘要，绝不联网解析 schema。

## 安装与验证

Python 3.12+，依赖唯一入口 `pyproject.toml` / `uv.lock`。从本项目根目录执行（Windows）：

```powershell
uv sync --locked --extra dev --python 'C:/Users/Administrator/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
.venv/Scripts/ruff.exe check src tests/gateway_fixtures.py tests/test_gateway_http.py tests/test_gateway_boundaries.py tests/test_responses.py tests/test_responses_candidate.py tests/test_responses_native.py tests/test_usage_report.py
.venv/Scripts/ruff.exe format --check src tests/gateway_fixtures.py tests/test_gateway_http.py tests/test_gateway_boundaries.py tests/test_responses.py tests/test_responses_candidate.py tests/test_responses_native.py tests/test_usage_report.py
.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_gateway*.py' -v
.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_responses*.py' -v
.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_usage_report.py' -v
git diff --check
```

有其他 Python 3.12+ 时替换 `--python` 路径。Linux 将 `.venv/Scripts/` 换为 `.venv/bin/`。测试从 `.runtime/workspace-context.json` 读取主工作区，独立检出可显式设置 `TIANSHU_WORKSPACE`。所有测试配置发布者与录制上游都在 `tests/`，端口由操作系统临时分配，测试凭据是命名明确的虚构值；退出后关闭服务与连接。

## 服务入口

部署输入、接口头、拒绝语义及验证边界见[运行说明](docs/gateway-runtime.md)。需要操作方提供实际平台 URL、已登记目标 IP、凭据环境变量引用、客户端授权和数据库路径；没有可用配置时拒绝调用，没有内置模型或 API Key。

```powershell
.venv/Scripts/python.exe -B -m tianshu_gateway --settings .runtime/gateway-settings.json --port 8443 --tls-cert .runtime/server.crt --tls-key .runtime/server.key
```

`--local-test` 仅供显式隔离夹具：监听地址及登记目标 IP 必须全部为 loopback；普通启动要求入站 TLS 和 HTTPS 平台配置源。配置不是网关管理面：模型、策略、绑定只从平台发布的快照读取。未显式启用 native 时 Responses 返回 501，Anthropic 与 embedding 始终返回 501；状态引用在缺少可用黏性解析器时明确拒绝。数据库取舍见[TS-041 决定](docs/decisions/TS-041-diagnostics.md)，交付见[TS-041 交接](docs/handoffs/TS-041.md)。

## TS-040 历史离线验证

Python 3.12+。目标实验只用标准库；旧源码核对直接导入只读参考项目，使用其现有 `cryptography==50.0.1`，无 API Key、网络请求或服务安装。当前机器已核定 Python 3.12.14、cryptography 50.0.1，无需安装依赖。通用命令：

```text
python -B -m unittest discover -s tests -p test_protocol_lab.py -v
python -B -m unittest discover -s tests -p test_legacy_characterization.py -v
git diff --check
```

本机 Python 不在 PATH，PowerShell 从本任务根目录执行：

```powershell
& 'C:/Users/Administrator/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe' -B -m unittest discover -s tests -p test_legacy_characterization.py -v
```

离开分配 worktree 后，运行旧源码核对前显式设置 `LEGACY_ROUTING_ROOT` 为相同只读参考检出路径。缺参考目录/依赖会失败，不跳过后报全绿。源文件 SHA-256 存在 `tests/fixtures/legacy-source.json`；不要自动接受变化。

`tests/protocol_lab.py` 是可抛弃的规则实验，`tests/fixtures/native.json` 是人为构造的请求及流式片段，`test_legacy_characterization.py` 执行真实旧规划器。绿色测试同时表示“旧行为得到复现”和“实验符合拟定规则”，不表示旧实现符合 V2。流片段不是完整上游响应录制，虚构模型不证明任何接入商支持这些字段组合。

## TS-042 原生 Responses 路由

`src/tianshu_gateway/native.py` 消费已发布 `model-protocol/v1` 1.0.0，`src/tianshu_gateway/responses.py` 提供原生单次 HTTP 传输与旁路观察器。默认部署不注册 Responses：只有部署显式给出 `native_enabled` 与 `native_contract_directory` 时才注册 `POST /v1/responses` 与 `GET /internal/v1/native-model-requests/{request_id}`，否则该路径返回 501 `unsupported_operation`（native 合同信封）。模型配置仍只由平台发布；native 版本走 `POST /internal/v1/model-config/native/snapshot`，使用独立 native 表、独立 ledger 键空间（contract/principal/caller/namespace），不继承 Chat 版本或 Chat 授权。客户端身份不取请求正文或任意头，只取部署注册的 principal/service/namespace。

`preserve_client` 保留客户端原生 JSON bytes（model/reasoning/store/未知字段），状态引用明确拒绝，无默认回退与自动重放；SSE 仅旁路观察不改字节，观察超预算、未知/无法解析事件或缺终态只把记账降级为 unknown 而不截断已收到的字节；只有真正的传输失败、超时、取消、断流与凭据反射才中止尝试。部署 JSON 的列表字段按数组书写，可直接启动真实 CLI。失效注册（撤销/过期/缺权限/无 native 版本授权）读取自己的回执也拒绝，native 版本撤销则保留历史审计。部署字段与边界见[运行说明](docs/gateway-runtime.md)，内部模块边界见[Responses 说明](docs/responses-internal.md)。

本块验证：`.venv/Scripts/python.exe -B -m unittest discover -s tests -p 'test_responses*.py' -v`（组件、候选包与 native 路由）；旧 Chat 回归沿用 `test_gateway*.py`。静态检查与格式检查覆盖 `src tests/gateway_fixtures.py tests/test_gateway_http.py tests/test_gateway_boundaries.py tests/test_responses.py tests/test_responses_candidate.py tests/test_responses_native.py`。以上只用 loopback 替身，不调用真实模型，也不代表平台生产者或生产接入已验收。

## TS-043 用量与延迟诊断

只读聚合建立在既有回执之上：`src/tianshu_gateway/usage.py` 在受限选择集内用 SQL 统计，`usage_report.py` 是本地运维 CLI，`server.py` 新注册两个读端口。

```powershell
.venv/Scripts/python.exe -B -m tianshu_gateway usage-report --settings .runtime/gateway-settings.json --service companion --view attempts --limit 50
```

- `GET /internal/v1/model-usage`（Chat）与 `GET /internal/v1/native-model-usage`（native，关闭时 501）按当前认证身份只返回自己的行，参数为 `view=summary|attempts`、`since`/`until`（ISO-8601 `Z`）、`limit`（≤5000）、`offset`（≤100000），窗口半开且上限 366 天，越界/未知/重复参数 400。
- 计数分为成功/失败/取消/未知并恒等于总数；用量只聚合归一化 input/output tokens，来源显式（`upstream_json_usage`/`upstream_stream_usage`/`not_reported`/`unobserved`），缺失按缺失计数而不是 0，供应商原始结构不求和。
- 延迟区分请求总耗时与首上游字节/首事件/首输出；未观察记 null。总耗时含背压，不代表模型生成耗时。
- 新私有表 `request_metrics`/`native_request_metrics` 首次在既有部署上启动时先整库备份 `<ledger>.ts043-backup` 再建表，既有表与旧回执不变，缺新表的历史行计入 `coverage.unmetered_total`。不新增全量内容日志、后台遥测、全表内存扫描或费用估算，也不修改根 contracts 与依赖锁。
- 命令、参数与验证界限见[运行说明](docs/gateway-runtime.md)，取舍见[TS-043 决定](docs/decisions/TS-043-usage-report.md)，交付见[TS-043 交接](docs/handoffs/TS-043.md)。
