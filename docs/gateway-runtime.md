# TS-041 运行与集成边�?

本切片消费发布提�?`102d347` 中的合同 1.0.0。manifest �?UTF-8、CRLF→LF 计算�?SHA-256 �?`81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1`；model schema �?`9ffa9055aca897ff57292555f37674d79df687d1d6c062df48267527a8ff3a7c`。不复制旧控制平面，不创建本地模型配置发布接口�?

## 部署输入

`--settings` 为部署方持有�?JSON，内容仅含服务启动和授权引用；不能放 Key 原值。下面只是字段示意，地址、版本、IP 和路径均需实际登记，不是运行默认值：

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

部署输入没有 model、reasoning 策略或业�?binding 内容。`clients` 只是服务器授权的 provider/version 选择范围，外�?native 客户端设 `internal: false`；不能凭客户端头扩大范围。每服务独立 Bearer 凭据，从注册环境变量逐次读取，缺�?非法值拒绝认证；上游凭据由当前快照中�?`credential_ref` 单独解析，客户端 token 不作为上�?Key。环境变量存储是首轮服务器适配器，不代表已完成生产密钥管理、加密备份或轮换传播。Python 字符串不承诺内存清零�?

目标 URL 必须精确匹配部署登记，凭据不出现�?URL 中。IP 列表由操作方审查后写入；连接使用这些固定地址，域名用�?HTTP Host / TLS 校验，不由请求正�?普�?DNS 刷新扩张目标。更�?IP 要重新审查部署输入。所有重定向关闭，系统代理和 Cookie 存储关闭。上游明确登记的私网 HTTP 需�?`allow_private_http: true`；普通服务启动仍要求入站 TLS �?HTTPS 平台源。测试模式只允许 loopback。生�?TLS 链、私网目标所有权、DNS/IP 更新流程及网络层防护未验收�?

运行默认上限是全局 16、每 provider 4、请求正�?1 MiB、非流响�?8 MiB、上传读�?5 秒、上游总时限最�?180 秒（�?binding.timeout_ms 与此上限较小值）。这些是本切片部署默认，不是共享合同常量；字段分别为 `max_concurrent`、`max_provider_concurrent`、`max_request_bytes`、`max_response_bytes`、`request_read_timeout`、`max_timeout_ms`。无排队，容量不足返�?429；SSE 观察缓冲最�?256 KiB/事件，超过则不能判成功。客户端慢读受总时限与异步写入背压约束，尚未做生产负载容量评估�?

## 配置来源及缓�?

实际客户端调�?`POST {platform_base_url}/internal/v1/model-config/snapshot`，使�?model.config_request，固定非 null 版本。query.origin.assertion_ref �?`platform_origin_env` 读取，由平台验证，网关不自行签发来源。平台配置授权与来源续期尚待 TS-012 接入；环境变量读取只证明本模块能传递发布合同，不证明实�?issuer 可用�?

消费时校�?schema、返�?request_id、指定版本、发布时�?有效期、唯一 provider 和每 workload 唯一 binding，并要求 binding.provider_id 指向实际选择�?provider，binding.model_id �?provider 目录模型一致。`validate_snapshot` 可供发布方审查同样约束，但本项目不发布配置，TS-012 仍必须在自己的发布事务中落实�?

同版内容摘要持久保存，后续同版改内容返回 409；query request_id 不参与摘要。默认最多缓�?32 版，30 秒再验证（`config_refresh_seconds` 可改）。平台网络不可达/5xx 时，只能继续使用仍在 usable_until 内的已验证同版。未知版本、错关联、无效内容和 4xx 不用缓存掩盖。已收到�?403/410 拒绝会持久标记该版不可再用；部署 `revoked_versions` 和内�?`ConfigCache.revoke` 提供同一拒绝入口，没有未发布的撤销 HTTP wire 接口�?

撤销推�?源凭据更新协议未实现，不能声称瞬时离线撤销；现有平台读取拒绝、操作方显式已知撤销�?usable_until 是首轮界限。平台离线期间缓存最多存活到已发布有效期；进程重启不恢复快照缓存，必须重新联系平台，摘要、轮次固定版本和撤销记录保留。已开始的请求继续使用开始时的版本；不改�?latest�?

## 原生入口和诊�?

- `POST /v1/chat/completions` 接受原生 JSON。内部服务必须带 `X-Request-ID`、`X-Tianshu-Config-Version`、`X-Tianshu-Workload: companion.text`、`X-Tianshu-Turn-ID`；客户端服务身份�?Bearer 认证决定。外部客户端使用部署固定版本/provider，提交这些内部头会被拒绝�?
- `GET /internal/v1/model-requests/{request_id}` 只返回当前认证服务自己记录的 model.route_receipt，不返回正文或凭据引用。查询他服务 ID 与不存在都为 404。无路由前校验失败没有成功回执�?
- 同一服务/轮次首次上游调用持久固定版本；同一服务 request_id 再提交返�?409，不重放上游。查询已有回执用于核对；�?ID 冲突保护不等于整�?native API 的幂等重放服务，也不替代业务幂等键�?
- `POST /v1/responses` 只在部署显式启用 native 时注册（见下节），否则与 `/v1/messages`、`/v1/embeddings` 一样明确返�?501。其他未实现路径不转换为 Chat。消息中已识别的 response/conversation/file/container 引用无法解析黏性时拒绝；工具与输出 schema 的字段声明不当作状�?ID。未识别供应商方言不承诺状态支持�?

默认 `preserve_client` 保留客户端指定模型和原生 JSON 字段。只�?`default_if_absent` 补缺失，`force` 按发布策略覆盖；内部 binding 可补缺失 model。缺 model �?requested_model �?null；显�?model:null、空白或控制字符拒绝，force 也不修复非法 null。`reasoning_effort` �?null/false/0/未知字符串均是值，不当缺省；其他原生思�?工具扩展保留于正文。回执按发布关系只投�?reasoning_effort，不把未知方言改为通用 effort 枚举。策略仅修改指定顶层值的 JSON 字节区间，其余内容（含高精度数值表示）保持。重复键、非 UTF-8、NaN/Infinity 和超�?Python 有限浮点范围的数值拒绝�?

仅转发上游协议需要的 Content-Type、Accept、OpenAI-Beta，以及单独解析的 Authorization；内部关联头、客户端 Key/Cookie/任意头不外发。OpenAI-Organization / OpenAI-Project 会改变账号命名空间，本切片明确拒绝客户端指定，后续需版本化配置授权；不能悄悄沿用其他租户�?

非流成功响应保留上游 JSON bytes。流逐块读取/写入原生 bytes，不拼接替代提供商，不重建工具参�?finish_reason/usage/[DONE]。HTTP chunk 的物理边界由协议栈决定，验收保证原始字节序列及提前交付，不宣�?TCP 包边界恒等。旁�?SSE 观察要求所�?n 个选择�?finish_reason 且收�?[DONE] 并正常结束；错误事件、畸�?过大事件、缺终止、断线和超时不能�?succeeded。已经开始的 SSE 失败直接中止连接，不伪�?[DONE] 或补业务错误帧。取消会关闭上游连接；已经可能执行的结果�?unknown，不能承诺供应商回滚计算或工具�?

上游�?2xx 保留 HTTP 状态，只回固定脱敏错误结构，不回原始错误正文、Location、Cookie 或任意响应头。已知凭据反射会被阻断，包括跨读取块拆开的原�?JSON 转义形式；正常字节通道最多保留最长凭据长度减一的尾部以检查分片。它不声称识别任意编�?变形后的秘密。请求正文和上游异常文本不入日志；回执中�?native_usage �?reasoning 独立递归脱敏�?

未知 usage �?null；部分只记录上游给出的合法非负整数，0 只有明确报告时才记录。usage_complete 需要完成响应且 input/output 均已知；native_usage 保留供应商原始统计结构（秘密脱敏）。不补�?token、不计算或虚构价格，合同没有 cost 字段�?

## 验证界限

`test_gateway_http.py` 使用独立临时监听端口启动平台替身、录制上游及真实网关，另启动一次命令行子进程。它核对真实 HTTP 到达的正文和头、非�?工具 SSE、逐字�?UTF-8 分片、提前流交付、错误状态、断流终止、超时、客户端取消、连�?槽释放、大小限制、版�?撤销/凭据失败、独立查询、SQLite 重启与写入故障。实�?I14 请求、原生输出和路由回执还喂给发布方 `contracts/validate.py` 的关系检查。纯边界测试另验�?schema 摘要、数�?字段策略、所�?SSE 双分片边界与登记目标限制�?

这不�?TS-050 多产�?L0，也不是 L1 或生产验收。下一步是协调 TS-012 的真实配置读取、来源引�?授权、撤销信号，之后才进行陪伴文字链联合验证。生�?PostgreSQL、TLS/私网审查、真实模型能�?价格与平台发布授权均单独验收�?

传输实现核对依据：[aiohttp 客户端文档](https://docs.aiohttp.org/en/stable/client.html)、[服务端取消与异步处理](https://docs.aiohttp.org/en/stable/web_advanced.html)。这些资料仅支持本项目采用的传输 API，不替代运行证据�?

## TS-042 原生 Responses 运行边界

消费已发�?`contracts/model-protocol/v1` 1.0.0（manifest LF SHA256 `52711a71de56dbceebd1d5d96b2baf59a2d9551168029972d59111480f815141`）。默认关闭：只有显式给出下面四项才注册路由，缺任一项在启动时报配置错误，不会“看起来可用”�?

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

- 路由：`POST /v1/responses`（原�?JSON 或原�?SSE）与 `GET /internal/v1/native-model-requests/{request_id}`。native 关闭时前者返�?501 `unsupported_operation`（native 合同信封），回执路径�?404�?
- 身份：`principal_id`、`caller_service`、`credential_namespace` 只来�?`native_clients` 注册；请求体、`metadata`/`user` 字段或任意客户端头都不构成身份。native 凭据�?Chat 凭据互不通用，注�?`expires_at`/`revoked` 或缺�?`config.snapshot` 权限即拒绝。部�?JSON 的列表字段按数组书写（`provider_ids`/`native_config_versions`/`permissions`/`config_versions`），元素类型与版本仍逐个校验；部署文件与 `dataclasses.asdict` 结果可直接互换�?
- 回执读取：仍�?(contract, principal, caller, namespace) 归属校验，跨主体/服务/namespace 一�?404；但**失效注册**（revoked、过期、缺 `config.snapshot` 权限�?`native_config_versions` 为空）不是可用身份，读取自己的历史回执也返回 403。native **版本**撤销是另一件事：它阻止新的选路，但不抹除已有审计行，因此回执读取不查询版本撤销状态�?
- 版本：可信内部服务用 `X-Tianshu-Native-Config-Version`（正整数，须�?`native_config_versions` 内）；外部注册可用部署固定的 `native_config_version`，为 null 时由平台选授权最新版，网关仍会校验返回值在授权集合内。Chat 路由拒绝 native 版本头，native 路由拒绝 Chat �?`X-Tianshu-Config-Version`/`X-Tianshu-Workload`，两套相同整数不代表同一配置�?
- 配置：`POST {platform_base_url}/internal/v1/model-config/native/snapshot`，`native_config_request` �?`query`/`native_config_version`/`contract`，origin 引用仍取�?`platform_origin_env`。平�?403/410 会按该主�?版本持久拒绝�?xx 只允许继续使用已验证且未过期的同版缓存；404/关联 ID 不符/窗口过期分别返回 404/409/503，不降到 Chat�?
- 路由与保真：`preserve_client` 只做精确模型匹配（客户端 model 必须等于 provider/binding model，缺失或不同�?400），不补默认、不替换；请求原�?bytes 直发上游 `{base_url}/responses`，只加协议所需 `Content-Type`/`Accept`/`Accept-Encoding: identity` 与单独解析的上游凭据。状态引用（previous_response_id/conversation/prompt/缓存比较 ID/item_reference/文件容器引用）在发送前拒绝�?
- 结果与回执：native 回执只写 `native_config_version` �?`openai-responses`，不�?Chat 版本或协议；`requested_reasoning`/`effective_reasoning` 只投影原�?reasoning，`applied_policies` 为空，`fallback_used=false`。上�?2xx JSON/SSE bytes 原样回传，上游错误保留状态与安全原生 JSON（不转发 Location/Set-Cookie/Cookie，不套本地信封）。断流、取消、发送后超时、落盘失败记 unknown 且不自动重放；超时诊�?reason �?`timeout_unknown`�?
- 观察与透传：observer 是旁路。上游正常结束时，观察超预算（每事件 256 KiB）、遇到未知或无法解析事件、或没有终态，都只把记账降级为 unknown（reason `observation_incomplete`），已收到的字节继续完整透传，不截断、不伪造终态、不追加本地事件。只有真正的传输失败（读错误、断流、压缩体、错�?content type、超时、取消、超大、无法解析的正文）与反射凭据才中止尝试�?
- 回执读取�?(contract, principal, caller, namespace) 归属校验，且�?Chat 账本、Chat 撤销链完全分开：撤销 Chat 版本 7 不影�?native 7，反之亦然�?
- 上限沿用现有 `max_request_bytes`/`max_response_bytes`/`max_concurrent`/`max_provider_concurrent`/`max_timeout_ms`，上游总时限取 binding.timeout_ms 与该上限较小值�?
- 验证界限：`tests/test_responses_native.py` 用独�?loopback 端口启动平台替身（同时提�?Chat �?native 快照）、录制上游与真实网关，并�?`dataclasses.asdict`→JSON 的部署文件启动真�?CLI 子进程；覆盖 JSON/SSE 字节保真、发布方 `validate.py` �?`exchange()` 关系、错误状态、观察超预算/未知事件不截断、断�?取消/超时、部�?未知/零用量、撤权两个方向、失效身份回执拒绝与版本撤销保留审计、回执隔离、同号不串用与秘密反射；不代表平台生产者已实现、也不代表生产可用。当前接口在根包中仍�?`runtime_disabled_until_joint_acceptance`�?

## TS-043 用量与延迟诊断运行边�?

在既有回执之上新增一�?*只读**聚合，不改根合同、不改既有回执字段、不新增公网未认证端点、不估费用、不写内容日志、不做全表内存扫描�?

- 读端口：`GET /internal/v1/model-usage`（Chat 键空间）�?`GET /internal/v1/native-model-usage`（native 键空间）。两者都要求 Bearer 认证：前者用已注�?Chat 客户端的独立凭据，后者用已注�?native 客户端凭据并再过一遍发布授权关系（revoked/过期/�?`config.snapshot`/`native_config_versions` 为空一�?403）；native 关闭时不注册该路径，返回 501 `unsupported_operation`。无凭据 401，响�?`Cache-Control: no-store`�?
- 参数：`view=summary|attempts`（默�?`summary`）、`since`/`until`（ISO-8601、必须带 `Z`、UTC）、`limit`�?..5000，默�?500）、`offset`�?..100000，默�?0）。其他参数名、重复参数、非规范数字（如 `01`、`1.0`、`-1`）一�?400。窗口半开 `[since, until)`，`until` 默认取当前时刻，跨度上限 366 天，默认窗口 24 小时；因此与查询同一毫秒完成的尝试可能落在默认窗口之外，需要精确覆盖时显式�?`until`�?
- 身份与隔离：只按当前认证身份过滤（Chat �?`service`，native �?contract/principal/caller/namespace 四元组），不回显、不聚合其他调用方的行；键空间互不相通，Chat 凭据不能�?native 报告，native 凭据不能�?Chat 报告。响应含 `identity`、`window`、`coverage`、`counts`、`reasons`、`latency_ms`、`usage`、`notes`，`attempts` 视图另有逐次记录�?
- 计数：`total` 为本窗口扫描行数，`succeeded`/`failed`/`cancelled`/`unknown` 由持�?`reason` 与上游状态推出，四者按减法补齐，恒等于 `total`。`upstream_http_error` 只有 4xx �?failed，其余保�?unknown；`cancelled_unknown` 记取消，`timeout_unknown`/`incomplete_stream`/`observation_incomplete`/传输失败保持 unknown�?
- 用量：只聚合归一�?`input_tokens`/`output_tokens`。`usage_source` �?`upstream_json_usage`/`upstream_stream_usage`/`not_reported`/`unobserved`——`not_reported` 表示"已完整读到上游响应但它没报用�?，`unobserved` 表示"根本没读到可判定的响�?，两者都不是 0。缺失按 `missing` 计数而不是补 0；`usage_complete` 需要终态完成且 input/output 均已知。供应商原始 `native_usage` 只留在回执里，不参与求和（`vendor_fields_aggregated` 恒为 false）�?
- 延迟：`request_total_ms` 来自既有回执 `elapsed_ms`，含读取、上游传输与下游背压�?*不是模型生成耗时**；`first_upstream_byte_ms`/`first_event_ms`/`first_output_ms` 用同一单调起点在本产品私有表记录，未观察为 `null` 而非 0。`first_event_ms` 是第一条可解析 SSE 事件（注�?心跳不算），`first_output_ms` 需已识别的输出事件；非流请求两者为 `null`，不能用总耗时冒充首字延迟。窗口内 `p50`/`p95` 为精确最近秩百分位（在受限选择集内排序，不扫全表）�?
- 私有表与迁移：新�?`request_metrics`、`native_request_metrics`、`schema_migrations`（均�?ledger 内，非根合同）。启动时�?ledger 已存在且含既有核心表，先整库复制�?`<ledger>.ts043-backup` 再建新表；既有表结构、行与旧版本兼容性都不变，新表缺失的历史行计�?`coverage.unmetered_total`。备份路径不自动清理�?
- 诊断与转发的隔离：回�?UPDATE 与私有度�?INSERT �?*两个独立事务**，且回执在前。回执写失败仍然按既有语义上抛（权威记录不能被吞）；度量写失败只记一条不含任何值的有界告警 `metric_write_degraded table=...` 然后结束——不重试、不触发第二次上游调用、不回滚回执、不改变 `reason`、不截断已交付或正在交付�?JSON/SSE 字节，扫�?`usage` 来源与延迟观测代码本身也不会抛异常（投影失败退化为 `unobserved`）。这样的尝试没有私有行，只出现在 `coverage.unmetered_total` 里，绝不被算�?0 用量�?0 延迟�?
- 超范围用量：上游可以�?token 数报�?SQLite `INTEGER` 之外（如 `prompt_tokens=2**63`）。权威回执与响应原样保留该值；私有投影只索�?`0..10^15` 的值，超范围的一半不索引、不截断、不�?0，该�?`usage_complete` �?0 并进�?`missing`。窗口求和逐行按上限截断（第一版遗留的更大历史值计�?`clamped`），因此有界聚合不会整数溢出�?
- CLI：`.venv/Scripts/python.exe -B -m tianshu_gateway usage-report --settings <部署.json> [--database <ledger>] [--view attempts] [--since ...] [--until ...] [--limit N] [--offset N] [--compact] --service <已注册服�?`，native 改用 `--principal-id`/`--caller-service`/`--credential-namespace`。它只读打开 ledger（`mode=ro`），要求身份仍注册、仍被授权且凭据仍可解析，没�?全量导出"模式；退出码 0 成功�? 参数/部署/ledger 不可用（含尚未迁移的 ledger）�? 身份被拒。不会因为一次读取而迁移或改写 ledger�?
- 验证界限：`tests/test_usage_report.py` 用独�?loopback 端口启动平台替身（Chat �?native 快照）、录制上游与真实网关，覆�?JSON/SSE 用量来源、用量缺失、重复并发请求只计一次、取�?超时/4xx 分桶、首事件延迟与总耗时的区别、分页与上限、跨身份与跨键空间隔离、撤�?过期/凭据失效读取拒绝、重启后报告一致、正�?工具参数/Key/URL 不外泄，另启动真�?CLI 子进程核对同一 ledger 的只读读取与拒绝路径。故障隔离另有独立用例：用只对该�?INSERT �?ABORT �?TEMP 触发器令私有度量写入失败，断言 Chat/Responses �?JSON �?SSE 仍是 200 且字节完整、权威回执与 `reason` 不变、只调用上游一次、该尝试只出现在 `unmetered_total`，以�?`prompt_tokens=2**63` 时响应与回执保留原值而私有行留空。SQLite 通过不等�?PostgreSQL 通过；本段不代表生产容量、留存期限或平台侧配额已验收�?

## TS-044 有界调度运行边界

交互请求与后台请求在同一进程内有界调度：新增一�?*本地运行策略**，不改合同、不改回执字段、不�?JSON/SSE 字节、不新增上游调用、不改变既有 429/408/403 语义�?

```json
{
  "scheduling": {
    "max_in_flight": 16,
    "max_provider_in_flight": 4,
    "interactive_reserve": 2,
    "max_queue_length": 64,
    "wait_timeout_ms": 2000
  },
  "workload_bindings": {
    "bindings": {"companion": "interactive", "memory-index": "background"},
    "default_class": "interactive"
  }
}
```

- 字段：`scheduling` �?`workload_bindings` 都是可选部署字段。前者缺省即 `interactive_reserve: 0`，此�?*完全保持既有行为**（无排队、容量忙�?429 `queue_full`、不写任何准入行）；只有�?reserve 才启用有界池。`max_in_flight` 是硬全局上限，`max_provider_in_flight` 是每 provider 硬上限，两者独立；`max_queue_length` 是等待队列长度硬上限；`wait_timeout_ms` 是单个请求的等待期限�?
- 类别只由部署绑定决定：`workload_bindings.bindings` 把已认证服务映射�?`interactive`/`background`，未登记服务�?`default_class`。请求正文、模型名、协议或 `X-Tianshu-Workload` �?*都不�?*改变类别；该头只按发布合同做取值校验，取值与绑定不一致时仅记一�?`declared_workload_differs_from_binding` 观测，不影响任何准入决定�?
- 保留容量：后台最多只能占 `max_in_flight - interactive_reserve`（全局）与 `max_provider_in_flight - interactive_reserve`（每 provider）个槽位，因此交互请求始终能拿到保留槽位，而后台在池满时也不会被饿死（它有自己的固定额度）。同类内 FIFO，且交互等待者总是先于后台等待者被考虑�?
- 期限与取消：等待超过 `wait_timeout_ms` 返回 408 `timeout`；客户端断开按既有取消语义处理。两者都不产生额外上游调用、不留下队列项、不残留容量占用。超�?取消/队列�?重复键都以固定原因记入私有观测，`wait_ms` 只在私有表中，权威回执不受影响�?
- 许可与重验：拿到许可后、发送前，网关重新核验固定版本撤销状态与幂等键；被撤销的排队请求返�?403 而不是发出。一个启动键（Chat/native 都用 `X-Request-ID`）同时最多持有一个许可，重复的并发请求返�?400 `invalid_input`，不会为一次逻辑调用占用两个上游槽位�?
- 队长上限：队列满时立即返�?429 `queue_full`，不转发、不写回执行行；这保持与无排队部署相同的拒绝语义�?
- 重启语义：队列、计数与保留量都�?*进程内内存状�?*。进程重启即丢失等待顺序，等待中的调用方看到连接关闭，前一个进程不会补发；本切�?*不承�?*持久队列、跨进程协调或恢复。已经开始的尝试仍按既有回执核对�?
- 验证界限：`tests/test_ts044_scheduling.py` 先用调度模块自己的窄端口（注入时钟、假诊断端口、无 HTTP）验证容�?保留/公平/期限/取消/幂等释放，再用真实网关与录制上游验证交互能进、后台不饿死、跨协议共用同一池与 provider 额度、队长硬上限、超�?取消/撤权/重复键零额外上游、等待时长只在私有表、以�?CLI 子进程用同一部署文档装配同一池。全部为本机 loopback 夹具；不代表生产负载容量、生产限流策略或多进程部署已验收�?
