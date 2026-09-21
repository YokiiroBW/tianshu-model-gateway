# TS-103 部署说明：容器基础、只读探针与运行日志

本文件描述 TS-103 提供的部署面：镜像定义、挂载点、环境变量、存活/就绪探针、运行日志目录与维护命令。所有内容都可用隔离夹具验证；**本机没有容器运行时，镜像从未构建或运行**，因此本文件不声明构建通过、不声明生产部署完成。未验证项集中在文末“未验证清单”。

## 1. 镜像

`Dockerfile` 两阶段构建，基础镜像固定为 `python:3.12.12-slim-bookworm`（不使用浮动标签），构建期只从 `uv.lock` 解析：

```text
python -m uv sync --locked --no-dev --no-editable --python /usr/local/bin/python3.12
```

- `--locked`：锁文件与清单不一致时构建失败，而不是静默重新解析。
- `--no-dev`：镜像内不含开发依赖。
- `--no-editable`：安装为普通 site-packages，运行阶段不依赖源码目录。

运行用户是非 root 的 `tianshu`（uid/gid 10001），进程内不含任何默认端点、默认模型或 API Key。`STOPSIGNAL SIGTERM`，入口为：

```text
ENTRYPOINT ["python", "-m", "tianshu_gateway"]
CMD ["--settings", "/etc/tianshu/settings.json", "--host", "0.0.0.0", "--port", "8443",
     "--tls-cert", "/etc/tianshu/tls/server.crt", "--tls-key", "/etc/tianshu/tls/server.key"]
```

镜像默认要求入站 TLS：没有证书与私钥就不启动，`--local-test` 不会出现在镜像里（它只供显式隔离夹具使用，且强制全部 loopback）。

`.dockerignore` 排除 `.git`、`.venv`、`.runtime`、`.env*`、`tests`、`docs`、字节码与 `*.sqlite*`，因此构建上下文里没有本地状态、历史日志或凭据文件。

## 2. 挂载点

| 路径 | 用途 | 写者 |
| --- | --- | --- |
| `/etc/tianshu/settings.json` | 部署输入（只读挂载） | 操作方 |
| `/etc/tianshu/tls/server.crt`、`server.key` | 入站 TLS 材料 | 操作方 |
| `/etc/tianshu/tls/ca.pem` | 健康检查校验用的 CA | 操作方 |
| `/etc/tianshu/contracts/diagnostics/v1` | 已发布诊断合同包（只读） | 操作方 |
| `/var/log/tianshu` | 运行日志目录（持久卷） | 网关进程 |
| `/var/lib/tianshu` | 回执数据库目录（持久卷） | 网关进程 |

Dockerfile 只声明 `RUN mkdir -p` 与目录属主，不声明 `VOLUME`：卷由部署方显式挂载，避免匿名卷吞掉运行日志。`/var/log/tianshu` 与 `/var/lib/tianshu` 必须是可写持久卷，否则网关按第 4 节的规则显式降级或拒绝新业务。

## 3. 环境变量

| 变量 | 必需 | 说明 |
| --- | --- | --- |
| `TIANSHU_DIAGNOSTICS_TOKEN` | 是（否则就绪恒为 503） | 就绪探针的独立凭据，与任何业务凭据不同源 |
| 部署 JSON `secret_references` 指向的变量 | 是 | 客户端授权、平台凭据、上游凭据 |
| `UV_*`、`PYTHON*` | 否 | 构建期与解释器行为，不承载业务配置 |

凭据只以环境变量引用出现在部署文档里（`secret_references` 的值是变量名，不是值）。运行日志、探针响应与健康检查都不会写出这些值。

## 4. 运行日志

`observability.log_directory` 是唯一必需字段；未配置即显式非持久模式（stderr + 一条 `log.non_durable`），就绪永远不为 ready。

| 字段 | 默认 | 允许范围 |
| --- | --- | --- |
| `log_directory` | 无（必填） | 绝对路径 |
| `max_directory_bytes` | 1 GiB | 32 MiB – 64 GiB |
| `probe_token_env` | `TIANSHU_DIAGNOSTICS_TOKEN` | 环境变量名 |
| `probe_budget_ms` | 1000 | ≥1 |
| `observation_validity_seconds` | 120 | >0 |

行为边界：

- 每次尝试在发往上游之前先写一条 `upstream.call_started` 并 `fsync`；等待落盘时不持调度锁、不持数据库事务、不持转发写。
- 目录预算耗尽或 IO 失败时，**新**记录被拒绝，该次新业务以固定 503 `dependency_unavailable` 拒绝；已经交付给客户端的尝试不会被重发，已存文件不会被删除、截断或覆盖。
- 内存队列 ≤1024 条且 ≤8 MiB，段 64 MiB；满时丢弃新记录并显式降级。本批次不实现任何删除/轮转，保留策略由后续批次与集中采集一起确认。
- 事件字段是闭合集合，不写正文、提示词、token、凭据、URL、异常文本或供应商原始结构。

### 维护命令

只有真实成功写入才能证明日志可用，探针不能触发恢复：

```text
python -m tianshu_gateway log-recovery-check --settings /etc/tianshu/settings.json
```

重新测量目录预算、真实写一条记录并 `fsync`，成功打印一行 JSON 并退出 0，写入失败退出 1，未配置日志目录退出 2。它不碰业务状态、不建回执行、不调用模型。

## 5. 探针

| 端点 | 凭据 | 语义 |
| --- | --- | --- |
| `GET /health/live` | 无 | 进程活着：200 `{"status":"alive"}` |
| `GET /health/ready` | `Authorization: Bearer $TIANSHU_DIAGNOSTICS_TOKEN` | 能否承接新业务 |

`/health/ready` 的响应只有 `status`、`service`、`checks` 三个字段，`checks` 恰好是八个固定键，取值只来自 `ok|failed|not_configured|not_verified|non_durable`。缺凭据 401，未配置凭据 503，第三个并发探针 429，超预算 503（八项全部按未测量处理）。响应里没有 `checked_at`、没有计数、没有路径、没有环境值、没有业务标识。

- 必要项：`configuration`、`contracts`、`runtime`、`ledger`、`logging`。任一项非 `ok` 即 `not_ready`。
- 参考项：`platform`、`model`、`native`。`platform`/`model` 只在内部有效期内有**真实**成功观测才 `ok`，到期自动回 `not_verified`；`native` 未配置时为 `not_configured`。它们不影响基础 Chat 可用性。
- 探针只读：不写日志、不建目录、不迁移、不开新数据库连接、不刷新配置缓存、不占用业务容量、不建回执行。

编排建议：`livenessProbe` 指向 `/health/live`（不打 `/health/ready`，避免依赖抖动导致重启），`readinessProbe` 指向 `/health/ready` 并注入独立凭据。`HEALTHCHECK` 与 `scripts/healthcheck.py` 只使用 `/health/live`，仅标准库，强制 TLS 校验（`--cacert`），不发送任何凭据；`--allow-http` 只用于显式 loopback 诊断，镜像默认不使用。

## 6. 部署文档示例

`config/container.example.json` 是可加载的真实部署文档，不是示意片段：

```text
python -m tianshu_gateway --settings /etc/tianshu/settings.json \
  --tls-cert /etc/tianshu/tls/server.crt --tls-key /etc/tianshu/tls/server.key
```

该示例在 POSIX 上通过 `Settings.validate()`；其绝对路径（`/var/log/tianshu` 等）在 Windows 上按主机文件系统规则判定，因此 `tests/test_container_runtime.py` 在 Windows 只断言结构与可加载性，不断言绝对路径。

## 7. 未验证清单

以下项目在本批次**没有**被验证，交付与交接记录必须保持这一表述：

1. **镜像从未构建、从未运行**：本机没有可用的容器运行时（`docker` 不存在），`docker build`、`docker run`、`HEALTHCHECK` 实际执行、非 root 用户实际生效、卷权限、`STOPSIGNAL` 实际行为都未验证。`tests/test_container_runtime.py` 只验证 Dockerfile 文本性质、健康检查脚本对真实本地 TLS 服务的实际行为，以及示例文档的结构。
2. **未部署到任何环境**：没有 NAS、没有生产容器、没有真实账号、没有付费调用。
3. **保留与集中采集未确认**：段轮转/删除策略、集中采集端点与留存期限属于后续批次；本批次只保证“不删除、不丢旧记录、显式降级”。
4. **平台生产者侧未适配**：网关会向已登记的内部平台快照调用附带 `X-Tianshu-Correlation-Id`，平台生产者需要接受或忽略该头；这属于跨产品依赖，不在本卡内修改。
5. **联合验收未完成**：`contracts/diagnostics/v1` 仍为 `development_frozen_pending_joint_acceptance`；本卡不代替协调者的联合验收结论。
6. **无后台周期任务**：网关没有周期性后台工作，因此“后台周期实际工作开始结束”一行在合同映射中记为“不适用”，以源码与事件目录为证据，不用事件伪造。
