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
- `POST /v1/responses`、`/v1/messages`、`/v1/embeddings` 明确返回 501。其他未实现路径不转换为 Chat。消息中已识别的 response/conversation/file/container 引用无法解析黏性时拒绝；工具与输出 schema 的字段声明不当作状态 ID。未识别供应商方言不承诺状态支持。

默认 `preserve_client` 保留客户端指定模型和原生 JSON 字段。只有 `default_if_absent` 补缺失，`force` 按发布策略覆盖；内部 binding 可补缺失 model。缺 model 的 requested_model 为 null；显式 model:null、空白或控制字符拒绝，force 也不修复非法 null。`reasoning_effort` 的 null/false/0/未知字符串均是值，不当缺省；其他原生思考/工具扩展保留于正文。回执按发布关系只投影 reasoning_effort，不把未知方言改为通用 effort 枚举。策略仅修改指定顶层值的 JSON 字节区间，其余内容（含高精度数值表示）保持。重复键、非 UTF-8、NaN/Infinity 和超出 Python 有限浮点范围的数值拒绝。

仅转发上游协议需要的 Content-Type、Accept、OpenAI-Beta，以及单独解析的 Authorization；内部关联头、客户端 Key/Cookie/任意头不外发。OpenAI-Organization / OpenAI-Project 会改变账号命名空间，本切片明确拒绝客户端指定，后续需版本化配置授权；不能悄悄沿用其他租户。

非流成功响应保留上游 JSON bytes。流逐块读取/写入原生 bytes，不拼接替代提供商，不重建工具参数/finish_reason/usage/[DONE]。HTTP chunk 的物理边界由协议栈决定，验收保证原始字节序列及提前交付，不宣称 TCP 包边界恒等。旁路 SSE 观察要求所有 n 个选择有 finish_reason 且收到 [DONE] 并正常结束；错误事件、畸形/过大事件、缺终止、断线和超时不能标 succeeded。已经开始的 SSE 失败直接中止连接，不伪造 [DONE] 或补业务错误帧。取消会关闭上游连接；已经可能执行的结果记 unknown，不能承诺供应商回滚计算或工具。

上游非 2xx 保留 HTTP 状态，只回固定脱敏错误结构，不回原始错误正文、Location、Cookie 或任意响应头。已知凭据反射会被阻断，包括跨读取块拆开的原文/JSON 转义形式；正常字节通道最多保留最长凭据长度减一的尾部以检查分片。它不声称识别任意编码/变形后的秘密。请求正文和上游异常文本不入日志；回执中的 native_usage 和 reasoning 独立递归脱敏。

未知 usage 为 null；部分只记录上游给出的合法非负整数，0 只有明确报告时才记录。usage_complete 需要完成响应且 input/output 均已知；native_usage 保留供应商原始统计结构（秘密脱敏）。不补总 token、不计算或虚构价格，合同没有 cost 字段。

## 验证界限

`test_gateway_http.py` 使用独立临时监听端口启动平台替身、录制上游及真实网关，另启动一次命令行子进程。它核对真实 HTTP 到达的正文和头、非流/工具 SSE、逐字节 UTF-8 分片、提前流交付、错误状态、断流终止、超时、客户端取消、连接/槽释放、大小限制、版本/撤销/凭据失败、独立查询、SQLite 重启与写入故障。实际 I14 请求、原生输出和路由回执还喂给发布方 `contracts/validate.py` 的关系检查。纯边界测试另验证 schema 摘要、数值/字段策略、所有 SSE 双分片边界与登记目标限制。

这不是 TS-050 多产品 L0，也不是 L1 或生产验收。下一步是协调 TS-012 的真实配置读取、来源引用/授权、撤销信号，之后才进行陪伴文字链联合验证。生产 PostgreSQL、TLS/私网审查、真实模型能力/价格与平台发布授权均单独验收。

传输实现核对依据：[aiohttp 客户端文档](https://docs.aiohttp.org/en/stable/client.html)、[服务端取消与异步处理](https://docs.aiohttp.org/en/stable/web_advanced.html)。这些资料仅支持本项目采用的传输 API，不替代运行证据。
