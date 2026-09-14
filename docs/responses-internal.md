# TS-042 原生 Responses 传输

状态：传输与公开路由已实现并接线；默认部署不注册 `/v1/responses`（返回 501），只有显式 `native_enabled` + 已发布 native 合同目录才开启。运行时只读取已发布的 `contracts/model-protocol/v1` 1.0.0；历史候选目录仅供追溯，不被运行时读取。部署字段、身份、版本、回执与错误码见[运行说明](docs/gateway-runtime.md)。

## 原生行为

`send_responses` 接收已解析目标、现有 RegisteredTargets、专用 aiohttp session、native ledger、原始 JSON bytes 与异步下游 sink。它仅发一次 POST `{base_url}/responses`，不重新序列化请求，不填 model/reasoning/store/stream 等默认值，不重试、不跟随重定向。异步 write 保留背压；总超时包括下游等待。会话须按现有 Gateway 创建方式禁用环境代理、自动解压与 cookie，使用注册地址 resolver。该模块不是配置发布者或鉴权入口：身份、版本与回执由 `native.py`/`server.py` 在调用前确定，调用者必须先写入 native 账本（`native_begin`），本模块只负责该行的收尾与可选的合同校验钩子。

ResponsesObserver 复用现有有界 SSE 分帧器，识别原生 `response.completed`、`response.failed`、`response.incomplete` 和 `error`；不要求 Chat 的 `[DONE]`，不转换输出项目或 delta，不重排未知事件。每个观察事件上限 256 KiB。**观察是旁路**：上游正常结束时，超预算、未知或无法解析的事件、缺失终态都只把记账降级为 unknown，已收到的字节继续完整透传（含安全尾缓冲），既不截断合法流也不伪造终态；只有真正的传输失败、超时、取消、连接断开与凭据反射才中止本次尝试。断流、终态后数据、冲突响应 ID、格式错误、取消及超时都不能写成功。连接关闭不证明远端生成取消成功。

HTTP JSON 错误保留状态和安全正文；原生 failed/incomplete JSON/SSE 完整交付但诊断不是成功。非 JSON HTTP 错误、压缩响应、**无法解析的**正文与反射凭据被封闭为固定 result_unknown；能解析但无法确认为原生响应的正文同样透传并记 unknown。复用 SecretGuard 延迟小段尾部，保证分片凭据不能泄漏；因此“字节保真”适用于合法且不包含已知凭据的正文。下游调用者必须在 send_responses 抛异常时中断已开始的 HTTP 流，不能附加伪造终止标记或返回普通成功 EOF。

诊断只保存本地调用者给定路由投影、状态、response ID 和 usage，不存原生请求或输出。完整/部分/缺失用量分开；0 是观测值，未知为 null，布尔值不当 token 数，计价不实现。响应 ID 记录不代表已有续接归属证明。落盘失败向调用者抛出，不伪装诊断成功。

## 明确边界

支持显式 model、文字与内联历史 input、function/custom 工具描述与调用输出、原生 reasoning/加密内容、未知字段。工具 schema 和业务字符串中的 file_id 不当作远端引用。

已知远端引用按原生位置拒绝：previous_response_id/conversation/prompt、缓存 comparison_response_id、item_reference（含省略 type 的 ID 引用）、文件/容器引用等。后台、文件/图像/音频、内置远程工具、WebSocket 与 retrieve/delete/cancel/compact 等生命周期入口不实现。store 保持客户端选择；stateless 表示不消费服务器引用，不表示网关强制供应商不存储。其他供应商与 embedding 不在本块范围。

接线现状：根独立合同已由协调者发布（`contracts/model-protocol/v1` 1.0.0），网关消费该包并已注册 `POST /v1/responses` 与 `GET /internal/v1/native-model-requests/{request_id}`，但默认关闭、只有部署显式启用才注册。网关验证认证 principal/namespace/provider/model/native 版本与有效期/撤销/绑定关系，并生成 route_context 与 native 回执。平台唯一 Models owner 的 native 快照生产者仍待其增量实现：正式发布状态在根包中仍为 `runtime_disabled_until_joint_acceptance`，本卡不声明联合验收或生产可用。

## 证据与来源

测试采用隔离 loopback HTTP 录制上游，原生字段与精确数字 bytes、单字节 SSE、多分片观察、错误、缺失/部分 usage、断流、取消、背压和凭据反射；另有真实 HTTP 网关级检查（native + 旧 Chat 同号不串用、撤权两向、回执归属隔离），并把实际录制的配置请求、快照、回执与上游/下游文档喂给发布方 `validate.py` 的 `exchange()` 关系逻辑。它们证明组件与 HTTP 替身行为，不代表真实平台 Responses 发布、模型兼容或生产费用测试。

2026-09-14 使用 OpenAI Docs 实际查阅：

- [Responses 创建参考](https://developers.openai.com/api/reference/python/resources/responses/methods/create)：原生请求形状和 background、引用参数。
- [原生流式事件](https://developers.openai.com/api/docs/guides/streaming-responses)：typed event 生命周期和 error。
- [对话状态](https://developers.openai.com/api/docs/guides/conversation-state)：手工重放 output 与服务器状态续接的区别。

精确路由粘滞和当前拒绝范围是天枢的安全边界，不是对供应商全部能力的描述。
