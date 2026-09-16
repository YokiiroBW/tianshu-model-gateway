# TS-041 运行与集成边界

本切片消费发布提交 `102d347` 中的合同 1.0.0。manifest 按 UTF-8、CRLF→LF 计算的 SHA-256 为 `81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1`；model schema 为 `9ffa9055aca897ff57292555f37674d79df687d1d6c062df48267527a8ff3a7c`。不复制旧控制平面，不创建本地模型配置发布接口。

## 部署输入

`--settings` 为部署方持有的 JSON，内容仅含服务启动和授权引用；不能放 Key 原值。下面只是字段示意，地址、版本、IP 和路径均需实际登记，不是运行默认值：

```json
{
  "contract_directory": "C:/deployment/contracts/text-dialogue/v1",
  "diagnostics_path": "C:/deployment/runtime/gateway.sqlite",
  "platform_base_url": "https://platform.example.invalid",
  "platform_credential_ref": "secret-ref:gateway/platform",
  "platform_origin_env": "TIANSHU_PLATFORM_ORIGIN_REF",
  "secret_references": {
    "secret-ref:gateway/platform": "TIANSHU_PLATFORM_TOKEN",
    "secret-ref:gateway/companion": "TIANSHU_COMPANION_TOKEN",
    "secret-ref:provider/account-a": "TIANSHU_UPSTREAM_TOKEN"
  },
  "targets": [
    {"base_url": "https://platform.example.invalid", "addresses": ["192.0.2.10"]},
    {"base_url": "https://model.example.invalid/v1", "addresses": ["192.0.2.20"]}
  ],
  "clients": [
    {
      "service": "companion",
      "credential_ref": "secret-ref:gateway/companion",
      "provider_id": "provider-a",
      "config_version": 7,
      "internal": true,
      "allowed_versions": []
    }
  ],
  "revoked_versions": []
}
```

部署输入没有 model、reasoning 策略或业务 binding 内容。`clients` 只是服务器授权的 provider/version 选择范围，外部 native 客户端设 `internal: false`；不能凭客户端头扩大范围。每服务独立 Bearer 凭据，从注册环境变量逐次读取，缺失/非法值拒绝认证；上游凭据由当前快照中的 `credential_ref` 单独解析，客户端 token 不作为上游 Key。环境变量存储是首轮服务器适配器，不代表已完成生产密钥管理、加密备份或轮换传播。Python 字符串不承诺内存清零。

目标 URL 必须精确匹配部署登记，凭据不出现在 URL 中。IP 列表由操作方审查后写入；连接使用这些固定地址，域名用于 HTTP Host / TLS 校验，不由请求正文/普通 DNS 刷新扩张目标。更新 IP 要重新审查部署输入。所有重定向关闭，系统代理和 Cookie 存储关闭。上游明确登记的私网 HTTP 需要 `allow_private_http: true`；普通服务启动仍要求入站 TLS 和 HTTPS 平台源。测试模式只允许 loopback。生产 TLS 链、私网目标所有权、DNS/IP 更新流程及网络层防护未验收。

运行默认上限是全局 16、每 provider 4、请求正文 1 MiB、非流响应 8 MiB、上传读取 5 秒、上游总时限最多 180 秒（取 binding.timeout_ms 与此上限较小值）。这些是本切片部署默认，不是共享合同常量；字段分别为 `max_concurrent`、`max_provider_concurrent`、`max_request_bytes`、`max_response_bytes`、`request_read_timeout`、`max_timeout_ms`。无排队，容量不足返回 429；SSE 观察缓冲最多 256 KiB/事件，超过则不能判成功。客户端慢读受总时限与异步写入背压约束，尚未做生产负载容量评估。

## 配置来源及缓存

实际客户端调用 `POST {platform_base_url}/internal/v1/model-config/snapshot`，使用 model.config_request，固定非 null 版本。query.origin.assertion_ref 从 `platform_origin_env` 读取，由平台验证，网关不自行签发来源。平台配置授权与来源续期尚待 TS-012 接入；环境变量读取只证明本模块能传递发布合同，不证明实际 issuer 可用。

消费时校验 schema、返回 request_id、指定版本、发布时间/有效期、唯一 provider 和每 workload 唯一 binding，并要求 binding.provider_id 指向实际选择的 provider，binding.model_id 与 provider 目录模型一致。`validate_snapshot` 可供发布方审查同样约束，但本项目不发布配置，TS-012 仍必须在自己的发布事务中落实。

同版内容摘要持久保存，后续同版改内容返回 409；query request_id 不参与摘要。默认最多缓存 32 版，30 秒再验证（`config_refresh_seconds` 可改）。平台网络不可达/5xx 时，只能继续使用仍在 usable_until 内的已验证同版。未知版本、错关联、无效内容和 4xx 不用缓存掩盖。已收到的 403/410 拒绝会持久标记该版不可再用；部署 `revoked_versions` 和内部 `ConfigCache.revoke` 提供同一拒绝入口，没有未发布的撤销 HTTP wire 接口。

撤销推送/源凭据更新协议未实现，不能声称瞬时离线撤销；现有平台读取拒绝、操作方显式已知撤销及 usable_until 是首轮界限。平台离线期间缓存最多存活到已发布有效期；进程重启不恢复快照缓存，必须重新联系平台，摘要、轮次固定版本和撤销记录保留。已开始的请求继续使用开始时的版本；不改走 latest。

## 原生入口和诊断

- `POST /v1/chat/completions` 接受原生 JSON。内部服务必须带 `X-Request-ID`、`X-Tianshu-Config-Version`、`X-Tianshu-Workload: companion.text`、`X-Tianshu-Turn-ID`；客户端服务身份由 Bearer 认证决定。外部客户端使用部署固定版本/provider，提交这些内部头会被拒绝。
- `GET /internal/v1/model-requests/{request_id}` 只返回当前认证服务自己记录的 model.route_receipt，不返回正文或凭据引用。查询他服务 ID 与不存在都为 404。无路由前校验失败没有成功回执。
- 同一服务/轮次首次上游调用持久固定版本；同一服务 request_id 再提交返回 409，不重放上游。查询已有回执用于核对；此 ID 冲突保护不等于整个 native API 的幂等重放服务，也不替代业务幂等键。
- `POST /v1/responses` 只在部署显式启用 native 时注册（见下节），否则与 `/v1/messages`、`/v1/embeddings` 一样明确返回 501。其他未实现路径不转换为 Chat。消息中已识别的 response/conversation/file/container 引用无法解析黏性时拒绝；工具与输出 schema 的字段声明不当作状态 ID。未识别供应商方言不承诺状态支持。

默认 `preserve_client` 保留客户端指定模型和原生 JSON 字段。只有 `default_if_absent` 补缺失，`force` 按发布策略覆盖；内部 binding 可补缺失 model。缺 model 的 requested_model 为 null；显式 model:null、空白或控制字符拒绝，force 也不修复非法 null。`reasoning_effort` 的 null/false/0/未知字符串均是值，不当缺省；其他原生思考/工具扩展保留于正文。回执按发布关系只投影 reasoning_effort，不把未知方言改为通用 effort 枚举。策略仅修改指定顶层值的 JSON 字节区间，其余内容（含高精度数值表示）保持。重复键、非 UTF-8、NaN/Infinity 和超出 Python 有限浮点范围的数值拒绝。

仅转发上游协议需要的 Content-Type、Accept、OpenAI-Beta，以及单独解析的 Authorization；内部关联头、客户端 Key/Cookie/任意头不外发。OpenAI-Organization / OpenAI-Project 会改变账号命名空间，本切片明确拒绝客户端指定，后续需版本化配置授权；不能悄悄沿用其他租户。

非流成功响应保留上游 JSON bytes。流逐块读取/写入原生 bytes，不拼接替代提供商，不重建工具参数/finish_reason/usage/[DONE]。HTTP chunk 的物理边界由协议栈决定，验收保证原始字节序列及提前交付，不宣称 TCP 包边界恒等。旁路 SSE 观察要求所有 n 个选择有 finish_reason 且收到 [DONE] 并正常结束；错误事件、畸形/过大事件、缺终止、断线和超时不能标 succeeded。已经开始的 SSE 失败直接中止连接，不伪造 [DONE] 或补业务错误帧。取消会关闭上游连接；已经可能执行的结果记 unknown，不能承诺供应商回滚计算或工具。

上游非 2xx 保留 HTTP 状态，只回固定脱敏错误结构，不回原始错误正文、Location、Cookie 或任意响应头。已知凭据反射会被阻断，包括跨读取块拆开的原文/JSON 转义形式；正常字节通道最多保留最长凭据长度减一的尾部以检查分片。它不声称识别任意编码/变形后的秘密。请求正文和上游异常文本不入日志；回执中的 native_usage 和 reasoning 独立递归脱敏。

未知 usage 为 null；部分只记录上游给出的合法非负整数，0 只有明确报告时才记录。usage_complete 需要完成响应且 input/output 均已知；native_usage 保留供应商原始统计结构（秘密脱敏）。不补总 token、不计算或虚构价格，合同没有 cost 字段。

## 验证界限

`test_gateway_http.py` 使用独立临时监听端口启动平台替身、录制上游及真实网关，另启动一次命令行子进程。它核对真实 HTTP 到达的正文和头、非流/工具 SSE、逐字节 UTF-8 分片、提前流交付、错误状态、断流终止、超时、客户端取消、连接/槽释放、大小限制、版本/撤销/凭据失败、独立查询、SQLite 重启与写入故障。实际 I14 请求、原生输出和路由回执还喂给发布方 `contracts/validate.py` 的关系检查。纯边界测试另验证 schema 摘要、数值/字段策略、所有 SSE 双分片边界与登记目标限制。

这不是 TS-050 多产品 L0，也不是 L1 或生产验收。下一步是协调 TS-012 的真实配置读取、来源引用/授权、撤销信号，之后才进行陪伴文字链联合验证。生产 PostgreSQL、TLS/私网审查、真实模型能力/价格与平台发布授权均单独验收。

传输实现核对依据：[aiohttp 客户端文档](https://docs.aiohttp.org/en/stable/client.html)、[服务端取消与异步处理](https://docs.aiohttp.org/en/stable/web_advanced.html)。这些资料仅支持本项目采用的传输 API，不替代运行证据。

## TS-042 原生 Responses 运行边界

消费已发布 `contracts/model-protocol/v1` 1.0.0（manifest LF SHA256 `52711a71de56dbceebd1d5d96b2baf59a2d9551168029972d59111480f815141`）。默认关闭：只有显式给出下面四项才注册路由，缺任一项在启动时报配置错误，不会“看起来可用”。

```json
{
  "native_enabled": true,
  "native_contract_directory": "C:/deployment/contracts/model-protocol/v1",
  "native_clients": [
    {
      "service": "native-companion",
      "credential_ref": "secret-ref:gateway/native-companion",
      "principal_id": "principal-a",
      "credential_namespace": "credential-namespace-a",
      "provider_ids": ["provider-a"],
      "native_config_versions": [7, 8],
      "native_config_version": 7,
      "permissions": ["config.snapshot"],
      "internal": true,
      "expires_at": "2026-10-01T00:00:00Z"
    }
  ],
  "revoked_native_versions": []
}
```

- 路由：`POST /v1/responses`（原生 JSON 或原生 SSE）与 `GET /internal/v1/native-model-requests/{request_id}`。native 关闭时前者返回 501 `unsupported_operation`（native 合同信封），回执路径为 404。
- 身份：`principal_id`、`caller_service`、`credential_namespace` 只来自 `native_clients` 注册；请求体、`metadata`/`user` 字段或任意客户端头都不构成身份。native 凭据与 Chat 凭据互不通用，注册 `expires_at`/`revoked` 或缺少 `config.snapshot` 权限即拒绝。部署 JSON 的列表字段按数组书写（`provider_ids`/`native_config_versions`/`permissions`/`config_versions`），元素类型与版本仍逐个校验；部署文件与 `dataclasses.asdict` 结果可直接互换。
- 回执读取：仍按 (contract, principal, caller, namespace) 归属校验，跨主体/服务/namespace 一律 404；但**失效注册**（revoked、过期、缺 `config.snapshot` 权限或 `native_config_versions` 为空）不是可用身份，读取自己的历史回执也返回 403。native **版本**撤销是另一件事：它阻止新的选路，但不抹除已有审计行，因此回执读取不查询版本撤销状态。
- 版本：可信内部服务用 `X-Tianshu-Native-Config-Version`（正整数，须在 `native_config_versions` 内）；外部注册可用部署固定的 `native_config_version`，为 null 时由平台选授权最新版，网关仍会校验返回值在授权集合内。Chat 路由拒绝 native 版本头，native 路由拒绝 Chat 的 `X-Tianshu-Config-Version`/`X-Tianshu-Workload`，两套相同整数不代表同一配置。
- 配置：`POST {platform_base_url}/internal/v1/model-config/native/snapshot`，`native_config_request` 含 `query`/`native_config_version`/`contract`，origin 引用仍取自 `platform_origin_env`。平台 403/410 会按该主体+版本持久拒绝；5xx 只允许继续使用已验证且未过期的同版缓存；404/关联 ID 不符/窗口过期分别返回 404/409/503，不降到 Chat。
- 路由与保真：`preserve_client` 只做精确模型匹配（客户端 model 必须等于 provider/binding model，缺失或不同即 400），不补默认、不替换；请求原始 bytes 直发上游 `{base_url}/responses`，只加协议所需 `Content-Type`/`Accept`/`Accept-Encoding: identity` 与单独解析的上游凭据。状态引用（previous_response_id/conversation/prompt/缓存比较 ID/item_reference/文件容器引用）在发送前拒绝。
- 结果与回执：native 回执只写 `native_config_version` 与 `openai-responses`，不写 Chat 版本或协议；`requested_reasoning`/`effective_reasoning` 只投影原生 reasoning，`applied_policies` 为空，`fallback_used=false`。上游 2xx JSON/SSE bytes 原样回传，上游错误保留状态与安全原生 JSON（不转发 Location/Set-Cookie/Cookie，不套本地信封）。断流、取消、发送后超时、落盘失败记 unknown 且不自动重放；超时诊断 reason 为 `timeout_unknown`。
- 观察与透传：observer 是旁路。上游正常结束时，观察超预算（每事件 256 KiB）、遇到未知或无法解析事件、或没有终态，都只把记账降级为 unknown（reason `observation_incomplete`），已收到的字节继续完整透传，不截断、不伪造终态、不追加本地事件。只有真正的传输失败（读错误、断流、压缩体、错误 content type、超时、取消、超大、无法解析的正文）与反射凭据才中止尝试。
- 回执读取按 (contract, principal, caller, namespace) 归属校验，且与 Chat 账本、Chat 撤销链完全分开：撤销 Chat 版本 7 不影响 native 7，反之亦然。
- 上限沿用现有 `max_request_bytes`/`max_response_bytes`/`max_concurrent`/`max_provider_concurrent`/`max_timeout_ms`，上游总时限取 binding.timeout_ms 与该上限较小值。
- 验证界限：`tests/test_responses_native.py` 用独立 loopback 端口启动平台替身（同时提供 Chat 与 native 快照）、录制上游与真实网关，并用 `dataclasses.asdict`→JSON 的部署文件启动真实 CLI 子进程；覆盖 JSON/SSE 字节保真、发布方 `validate.py` 的 `exchange()` 关系、错误状态、观察超预算/未知事件不截断、断流/取消/超时、部分/未知/零用量、撤权两个方向、失效身份回执拒绝与版本撤销保留审计、回执隔离、同号不串用与秘密反射；不代表平台生产者已实现、也不代表生产可用。当前接口在根包中仍为 `runtime_disabled_until_joint_acceptance`。

## TS-043 用量与延迟诊断运行边界

在既有回执之上新增一段**只读**聚合，不改根合同、不改既有回执字段、不新增公网未认证端点、不估费用、不写内容日志、不做全表内存扫描。

- 读端口：`GET /internal/v1/model-usage`（Chat 键空间）与 `GET /internal/v1/native-model-usage`（native 键空间）。两者都要求 Bearer 认证：前者用已注册 Chat 客户端的独立凭据，后者用已注册 native 客户端凭据并再过一遍发布授权关系（revoked/过期/缺 `config.snapshot`/`native_config_versions` 为空一律 403）；native 关闭时不注册该路径，返回 501 `unsupported_operation`。无凭据 401，响应 `Cache-Control: no-store`。
- 参数：`view=summary|attempts`（默认 `summary`）、`since`/`until`（ISO-8601、必须带 `Z`、UTC）、`limit`（1..5000，默认 500）、`offset`（0..100000，默认 0）。其他参数名、重复参数、非规范数字（如 `01`、`1.0`、`-1`）一律 400。窗口半开 `[since, until)`，`until` 默认取当前时刻，跨度上限 366 天，默认窗口 24 小时；因此与查询同一毫秒完成的尝试可能落在默认窗口之外，需要精确覆盖时显式传 `until`。
- 身份与隔离：只按当前认证身份过滤（Chat 为 `service`，native 为 contract/principal/caller/namespace 四元组），不回显、不聚合其他调用方的行；键空间互不相通，Chat 凭据不能读 native 报告，native 凭据不能读 Chat 报告。响应含 `identity`、`window`、`coverage`、`counts`、`reasons`、`latency_ms`、`usage`、`notes`，`attempts` 视图另有逐次记录。
- 计数：`total` 为本窗口扫描行数，`succeeded`/`failed`/`cancelled`/`unknown` 由持久 `reason` 与上游状态推出，四者按减法补齐，恒等于 `total`。`upstream_http_error` 只有 4xx 记 failed，其余保持 unknown；`cancelled_unknown` 记取消，`timeout_unknown`/`incomplete_stream`/`observation_incomplete`/传输失败保持 unknown。
- 用量：只聚合归一化 `input_tokens`/`output_tokens`。`usage_source` 取 `upstream_json_usage`/`upstream_stream_usage`/`not_reported`/`unobserved`——`not_reported` 表示"已完整读到上游响应但它没报用量"，`unobserved` 表示"根本没读到可判定的响应"，两者都不是 0。缺失按 `missing` 计数而不是补 0；`usage_complete` 需要终态完成且 input/output 均已知。供应商原始 `native_usage` 只留在回执里，不参与求和（`vendor_fields_aggregated` 恒为 false）。
- 延迟：`request_total_ms` 来自既有回执 `elapsed_ms`，含读取、上游传输与下游背压，**不是模型生成耗时**；`first_upstream_byte_ms`/`first_event_ms`/`first_output_ms` 用同一单调起点在本产品私有表记录，未观察为 `null` 而非 0。`first_event_ms` 是第一条可解析 SSE 事件（注释/心跳不算），`first_output_ms` 需已识别的输出事件；非流请求两者为 `null`，不能用总耗时冒充首字延迟。窗口内 `p50`/`p95` 为精确最近秩百分位（在受限选择集内排序，不扫全表）。
- 私有表与迁移：新增 `request_metrics`、`native_request_metrics`、`schema_migrations`（均在 ledger 内，非根合同）。启动时若 ledger 已存在且含既有核心表，先整库复制为 `<ledger>.ts043-backup` 再建新表；既有表结构、行与旧版本兼容性都不变，新表缺失的历史行计入 `coverage.unmetered_total`。备份路径不自动清理。
- CLI：`.venv/Scripts/python.exe -B -m tianshu_gateway usage-report --settings <部署.json> [--database <ledger>] [--view attempts] [--since ...] [--until ...] [--limit N] [--offset N] [--compact] --service <已注册服务>`，native 改用 `--principal-id`/`--caller-service`/`--credential-namespace`。它只读打开 ledger（`mode=ro`），要求身份仍注册、仍被授权且凭据仍可解析，没有"全量导出"模式；退出码 0 成功、2 参数/部署/ledger 不可用（含尚未迁移的 ledger）、3 身份被拒。不会因为一次读取而迁移或改写 ledger。
- 验证界限：`tests/test_usage_report.py` 用独立 loopback 端口启动平台替身（Chat 与 native 快照）、录制上游与真实网关，覆盖 JSON/SSE 用量来源、用量缺失、重复并发请求只计一次、取消/超时/4xx 分桶、首事件延迟与总耗时的区别、分页与上限、跨身份与跨键空间隔离、撤权/过期/凭据失效读取拒绝、重启后报告一致、正文/工具参数/Key/URL 不外泄，另启动真实 CLI 子进程核对同一 ledger 的只读读取与拒绝路径。SQLite 通过不等于 PostgreSQL 通过；本段不代表生产容量、留存期限或平台侧配额已验收。
