# model-protocol/v1 语义

此文档与 schema、正反例、接口目录共同构成发布候选。当前 `release_ready_unpublished`，全部接口 `runtime_disabled_until_joint_acceptance`。schema 通过不能替代以下跨文档关系、身份来源与运行状态检查。

## 配置唯一所有者与版本

平台既有 `Models` 是唯一配置 owner。原生配置使用独立表、独立严格递增的 `native_config_version`、不可变内容摘要、发布审计与撤销记录。旧 Chat 配置表和 `config_version` 不参与原生选路、去重、最高版本或撤销判断。相同原生版本重复发布仅在规范化内容摘要一致且未撤销时可去重；相同版本不同内容拒绝，不能覆盖历史。发布者必须具有既有 operator 发布权限，不能新建网关写配置服务。

所有新 wire 显式 `contract=model-protocol/v1`；协议为 `openai-responses`，workload 为 `native.responses`。提供商 ID 不重复、binding workload 不重复、每个 binding 的 provider 必须存在、binding model_id 与 provider model_id 相同。注册校验涵盖 protocol、精确 base_url、credential_ref、credential_namespace、模型、能力及受审地址；凭据仅用引用，禁止在配置放密钥。`fixture_only` 不可因离线测试通过改标 `verified_test_account`。

平台 snapshot 验证 service/config.snapshot 权限、可信 origin assertion、请求关联、配置摘要、发布时刻与有效期，禁止根据正文中的原生参数猜测协议，并读取**显式** `caller.native_config_versions`。该列表缺失视为空，绝不继承 `caller.config_versions`。指定 native_config_version 时必须在原生授权集合内且存在、未撤销、未过期；不能自行选择新版本。请求版本为 null 时，只从原生表中筛选该调用者有权读取、已发布、未撤销且当前有效的快照，再选最大 native_config_version；不能先取全局最高版本后越权读取，也不能降到旧 Chat。无可用原生快照必须失败，不能返回空配置并声称可用。

网关仅从该平台端口读取快照，验证合同/摘要依赖/版本/有效期与 provider 注册并构造路由，不接受客户端上传配置作为可信平台快照。一次执行固定所选原生版本；版本变化不触发重试或换线。

## 身份与参数所有权

route_context 是网关内部可信投影，不是客户端可提交的身份断言。principal_id、caller_service 来自认证结果；credential_namespace 来自获授权且匹配的 provider 配置。不得从请求 JSON、metadata、user 字段或任意客户端头信任这些身份。其 native_config_version 与快照/回执一致，protocol 与 provider 一致。诊断读取也按可信主体、服务、namespace 与请求归属授权，不能仅凭 request_id 获得他人回执。

本切片只有 preserve_client，fields 为空且 applied_policies 为空。客户端显式 model 必须满足 provider/binding 的精确模型约束；不同或缺失时拒绝，不补默认、不替换。reasoning 及其未知 effort 值不改成平台默认；不套用 Chat reasoning_effort 政策。store 的省略、true、false 均保留；stateless 是不消费服务器引用，不能偷偷注入 store=false。完整 instructions/input/tools/reasoning/未知字段按原生 JSON 保留，function/custom 的 schema、arguments、返回项目及 encrypted_content 不被缩减成文本。

## 支持范围与状态引用

只支持 POST Responses 的 HTTP JSON 和 SSE；stream 保持原始含义。background=true 拒绝；后台查询/取消/删除、input-items、compact、WebSocket、文件生命周期、图像音频、内置远程工具、Anthropic 和 embedding 都不在本切片内。

state_references 必须 reject。非 null previous_response_id、conversation、prompt 及其他已知协议状态引用（如 prompt_cache_options.comparison_response_id）明确拒绝；input 的 item_reference、文件/图像/音频项目及相应内容部分也拒绝。检查按协议位置和项目类型进行，不递归扫描任意 file_id/id 键：工具参数 schema、arguments、metadata 和用户文本中的同名业务字段不得误杀。function_call_output.call_id 是完整手工上下文的关联键，不一概作为服务端引用拒绝。未知扩展字段保真不等于授权新增服务端状态能力。

未来要允许状态引用，需独立发布并验收持久化归属证明，将原生引用绑定到 (authenticated principal_id, caller_service, credential_namespace, provider_id, exact base_url, model_id, native_config_version, protocol)。读取必须验证同主体/namespace、所有引用一致、精确旧配置仍获授权且未到期/撤销、凭据空间未改变，并具备重启后恢复与撤销/删除生命周期。未知、失效或冲突引用必须拒绝；客户端自报元数据不构成证明。不能按当前默认路由解释旧引用，不能跨提供商、模型或原生版本 fallback。

## 字节、结果与用量

observer 旁路读取，不能改写 HTTP 错误正文或 SSE 事件、空行、注释、字段顺序、编码和分片连接后的原始字节。未知事件保留；无法解析或观察预算耗尽不能破坏转发，也不能猜成功。收到明确 response.completed 才记录完成；failed/incomplete/error 如实非成功，缺终态、断流、取消、超时或观察不完整记录 unknown。取消本地请求只关闭当前连接，不证明远端生成被撤销。已发送请求不得自动重放，不能把两条上游流拼成一次回答。

usage 缺失为 null，部分计数保留且 usage_complete=false，不补 0；只有确实观察到完整 input_tokens/output_tokens 与 native_usage 才标完整。明确观察到 0/0 是合法零用量。native_usage 保留供应商原始字段；未知价格不当免费。response_id 仅用于诊断，不能据其返回就授予继续引用权限。回执必须保存实际 requested/effective 参数、可信主体、provider、namespace 和 native_config_version，不把旧 Chat 版本或 protocol 写入原生回执。

## 网关错误信封与 HTTP 状态

网关自生错误用 `model#error`，固定包含 contract、schema_version、request_id、code、execution_state、retryable=false。只有 result_unknown 的 execution_state 为 unknown；其余代码均为 not_started。发送后超时必须是 result_unknown/HTTP 502/unknown，诊断 reason 为 timeout_unknown，不能用 timeout/408 声称未执行。retryable=false 表示不自动重放。

| code | HTTP | execution_state | 使用条件 |
| --- | --- | --- | --- |
| invalid_input | 400 | not_started | 非法请求、缺 model 或形状非法 |
| payload_too_large | 413 | not_started | 请求体超过限额 |
| unauthorized | 401 | not_started | 未认证或凭据无效 |
| forbidden | 403 | not_started | 无原生版本/namespace/操作权限或配置已撤销 |
| not_found | 404 | not_started | 已授权命名空间内目标不存在 |
| version_conflict | 409 | not_started | 版本、摘要或绑定关系冲突 |
| idempotency_conflict | 409 | not_started | 请求标识已占用，禁止再次生成 |
| state_reference_unsupported | 409 | not_started | 消费服务器状态引用而本切片未支持 |
| unsupported_operation | 501 | not_started | 后台、内置工具或其他未实现操作 |
| unsupported_version | 400 | not_started | 请求合同或版本不受支持 |
| queue_full | 429 | not_started | 发送前容量不足 |
| dependency_unavailable | 503 | not_started | 发送前平台/有效原生配置/上游依赖不可用，含配置过期 |
| timeout | 408 | not_started | 发送前等待超时 |
| result_unknown | 502 | unknown | 请求可能已发送，包括发送后超时而结果不可确定 |

上述是网关自生错误，不能套用到提供商原生错误。上游原生错误是独立 raw 响应类型，保留其 HTTP status 与安全原生 JSON 正文；不套本地 error schema，不转发 Location/Set-Cookie/Cookie。原生 SSE 中的错误也按原始流字节透传。响应头或 SSE 已发送后不能再改 HTTP 状态或向流尾追加本地错误 JSON；只能结束连接并记录诊断。回执读取先按可信主体/namespace 检查归属，不能泄露其他主体是否存在该 request_id。
