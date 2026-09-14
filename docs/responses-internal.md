# TS-042 内部 Responses 传输

状态：内部可测试实现，公开 `/v1/responses` 保持 501。合同候选不可被运行时读取；现行配置服务和 Chat 源文件没有改动。首轮是传输组件验收，尚不是正式 Responses 网关路由验收。

## 原生行为

`send_responses` 接收已解析目标、现有 RegisteredTargets、专用 aiohttp session、SQLite Diagnostics、原始 JSON bytes 与异步下游 sink。它仅发一次 POST `{base_url}/responses`，不重新序列化请求，不填 model/reasoning/store/stream 等默认值，不重试、不跟随重定向。异步 write 保留背压；总超时包括下游等待。会话须按现有 Gateway 创建方式禁用环境代理、自动解压与 cookie，使用注册地址 resolver。该内部接口不是配置发布者或鉴权入口。

ResponsesObserver 复用现有有界 SSE 分帧器，识别原生 `response.completed`、`response.failed`、`response.incomplete` 和 `error`；不要求 Chat 的 `[DONE]`，不转换输出项目或 delta，不重排未知事件。每个观察事件上限 256 KiB，超过上限保持传输字节路径但不能宣布成功；最终报告 unknown 并由调用者中断下游。断流、终态后数据、冲突响应 ID、格式错误、取消及超时都不能写成功。连接关闭不证明远端生成取消成功。

HTTP JSON 错误保留状态和安全正文；原生 failed/incomplete JSON/SSE 完整交付但诊断不是成功。非 JSON HTTP 错误、压缩响应、非法 JSON 与反射凭据被封闭为固定 result_unknown。复用 SecretGuard 延迟小段尾部，保证分片凭据不能泄漏；因此“字节保真”适用于合法且不包含已知凭据的正文。下游调用者必须在 send_responses 抛异常时中断已开始的 HTTP 流，不能附加伪造终止标记或返回普通成功 EOF。

诊断只保存本地调用者给定路由投影、状态、response ID 和 usage，不存原生请求或输出。完整/部分/缺失用量分开；0 是观测值，未知为 null，布尔值不当 token 数，计价不实现。响应 ID 记录不代表已有续接归属证明。落盘失败向调用者抛出，不伪装诊断成功。

## 明确边界

支持显式 model、文字与内联历史 input、function/custom 工具描述与调用输出、原生 reasoning/加密内容、未知字段。工具 schema 和业务字符串中的 file_id 不当作远端引用。

已知远端引用按原生位置拒绝：previous_response_id/conversation/prompt、缓存 comparison_response_id、item_reference（含省略 type 的 ID 引用）、文件/容器引用等。后台、文件/图像/音频、内置远程工具、WebSocket 与 retrieve/delete/cancel/compact 等生命周期入口不实现。store 保持客户端选择；stateless 表示不消费服务器引用，不表示网关强制供应商不存储。其他供应商与 embedding 不在本块范围。

正式接线需先发布根独立合同，并由平台唯一配置 owner 增加明确包/版本适配；网关随后验证认证 principal/namespace/provider/model/版本与有效期、撤销、配额、路由及 receipt，才能调用该内部模块。当前没有第二份 runtime 配置、假认证主体或可用新 endpoint。候选的最小安全选择是保留客户端参数，拒绝不匹配目标与全部状态引用。

## 证据与来源

测试采用隔离 loopback HTTP 录制上游，原生字段与精确数字 bytes、单字节 SSE、多分片观察、错误、缺失/部分 usage、断流、取消、背压和凭据反射；旧 Chat 使用现有网关 HTTP 回归。它们证明组件与 HTTP 替身行为，不代表真实平台 Responses 发布、模型兼容或生产费用测试。

2026-09-14 使用 OpenAI Docs 实际查阅：

- [Responses 创建参考](https://developers.openai.com/api/reference/python/resources/responses/methods/create)：原生请求形状和 background、引用参数。
- [原生流式事件](https://developers.openai.com/api/docs/guides/streaming-responses)：typed event 生命周期和 error。
- [对话状态](https://developers.openai.com/api/docs/guides/conversation-state)：手工重放 output 与服务器状态续接的区别。

精确路由粘滞和当前拒绝范围是天枢的安全边界，不是对供应商全部能力的描述。
