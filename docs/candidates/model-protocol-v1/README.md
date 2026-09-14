# TS-042：model-protocol/v1 候选（未发布）

此目录是交协调者审查的兼容扩展提案，不能作为运行时合同来源或开启路由的依据。正式发布唯一位置是根 contracts/；本目录的 manifest 明确 candidate_unpublished，平台生产者与真实路由验收均待完成。仅提出原生 openai-responses；旧 text-dialogue/v1 1.0.0 的文件、Chat provider、companion.text 绑定、路由与配置端口保持原样。

## 最小版本边界

schema.json 的 provider、binding、config_request、config_response、route_context、route_receipt 独立于旧 model.json；仅引用既有 common.json 的身份/query/id 定义。config/route 显式 contract=model-protocol/v1，protocol=openai-responses，workload=native.responses。新 workload 名称是待协调批准的候选，不是现有入口。

模型配置唯一 owner 仍是平台。新包不能成为第二个发布服务，也不能把 Responses provider 混装到旧 Chat 快照。建议平台在同一 Models 所有者中提供显式新版本 snapshot 适配器，使用同一身份与撤销链；确切 URL 与持久化版本空间由协调者发布时确认。旧 /internal/v1/model-config/snapshot 始终校验旧包，不做请求正文猜测或悄悄改成联合 schema。

首轮模型与 reasoning 策略只允许 preserve_client + 空 fields，应用策略记录为空；客户端必须明确 model。配置 model_id 是可用目标约束：不一致明确拒绝，不能替换请求 model。不得把 Chat reasoning_effort 政策复制为 Responses reasoning。模型能力状态依旧 fixture_only/verified_test_account；离线录制替身不能升级为真实能力确认。

## 平台生产者增量（基线 f4dac45）

只读依据为已集成 services/platform/models.py 的 Models.validate_publication/publish/snapshot/view，以及 services/platform/server.py 与 projections.py 的公开端口声明；未读取 TS-014 可变代码。

1. 增加独立包加载/显式版本适配，保留现有平台配置唯一写者、递增不可变版本、内容摘要、operator 发布/撤销与审计。不要无版本覆盖已存快照；若新旧快照共用版本表必须显式区分合同，latest 查询也必须按合同限定。
2. provider 注册新增可核验的 protocol 与 state_references=reject 约束；继续匹配精确 base_url、credential_ref/namespace、model_id、能力标记与 reviewed_addresses。不能只验证模型名然后默许协议。
3. snapshot 重用原有 service/config.snapshot 权限、origin assertion、caller.config_versions、有效期、撤销与摘要检查。route_context 的 caller_service/principal_id/credential_namespace 必须来自可信认证和已发布配置，不能信任客户端头或模型正文 user/metadata。
4. config/binding/route/receipt 关系校验：合同和协议相同；版本一致；binding.provider_id 存在且唯一；workload 唯一；model_id 一致；provider namespace 与认证允许 namespace 一致。schema 校验不能替代这些关系或鉴权。
5. 交付双方共用的新快照 fixture 与正反例，验证旧 Chat 快照及入口仍成功，Responses 不能被旧 schema 接纳；新入口在合同发布、平台增量及网关消费就绪前保持关闭。不要使 TS-014 UI 接受未发布 wire。

## 原生与状态边界

本切片仅 POST Responses HTTP JSON/SSE。native_request 是有限边界 schema，不是 OpenAI 完整 schema；未知字段、instructions/input/tools/reasoning 及 function/custom 参数、手工重放的完整输出项目/加密 reasoning 内容保留。stream 的省略和显式 false、store 的省略/true/false都不改写；stateless 指不消费服务器引用，不能据此偷偷注入 store=false。推理强度未知值不本地替换。

schema 明确拒绝非 null previous_response_id/conversation/prompt、background=true 和内置工具；运行时语义检查还必须按协议位置拒绝 input 中 item_reference、文件/图像/音频项目及其内容部分，顶层其他已知服务器状态引用（例如 prompt_cache_options.comparison_response_id）。不能递归扫描任意键名 file_id/id 然后误杀工具 JSON schema、arguments、metadata 或用户文本。业务 function_call_output.call_id 是手工上下文关联，不能一概当服务器状态引用。未知字段透传不等于新增未知服务器状态能力得到授权；新增已知引用语义需补边界与验收。

未来支持引用时，网关必须持久化从响应/会话/项目/文件等原生引用到 (authenticated principal_id, caller_service, credential_namespace, provider_id, exact base_url, model_id, config_version, protocol) 的绑定，引用创建来源需有可信证据。必须验证同主体/namespace、所有引用归属一致、目标与旧快照仍获授权且未撤销/到期、凭据空间不变。未知、冲突、跨用户、跨提供商/模型/版本、失效快照、无法恢复映射均拒绝；不能按当前默认配置解释旧 ID，也不能跨线路 fallback。客户端给出的同名元数据不是归属证明。重启后的证明恢复、引用生命周期和删除/撤销规则必须作为未来独立验收，本候选只允许 reject。

不支持后台作业/retrieve/cancel/delete/input-items/compact/WebSocket、文件生命周期、内置远程工具、图像音频、Anthropic、embedding。应用层取消关闭当前连接，不代表远端撤销生成成功。未知执行结果不自动重放。

## 回执与验收

原生响应体和 SSE 字节原样传送，诊断 observer 只旁路读取。response.completed 成功、failed/incomplete 如实非成功、error 失败；流缺终态、断线、取消或观察预算耗尽记 unknown。usage 缺失为 null，部分计数不补 0；完整 0/0 是已观察的合法值；native_usage 保留提供商字段。HTTP 错误及原生流内错误保留正文，不拼接另一个上游结果。response_id 仅为诊断关联，不授予继续引用权限。

测试命令：`.venv/Scripts/python.exe -B -m unittest discover -s tests -p test_responses_candidate.py -v`。这里仅验证候选 schema/实例和与旧协议的隔离；运行时 HTTP、SSE 分片、断线取消、原生字节、Chat 回归由 TS-042 网关测试负责。正式发布需平台生产者和网关消费者共同跑已发布包，不能以候选 schema 通过替代真实平台。

2026-09-14 使用 OpenAI Docs 核对并实际打开 [Responses 创建 API](https://developers.openai.com/api/reference/python/resources/responses/methods/create)：previous_response_id 与 conversation 是服务器状态机制，instructions 不会自动继承；background 是独立模式，原生 input/reasoning/tools 结构不能有损转成 Chat。此处拒绝范围与精确粘滞要求是天枢本切片决策，不声称为 OpenAI 全部限制。

## 发布前需定稿的错误与版本约定

内部模块尚不发布错误 wire。拟议公开错误沿用固定字段 schema_version/request_id/code/execution_state/retryable 的模式，但必须由根接口目录明确属于新包。入口未启用保持现有 501；已知状态引用在发送前拒绝为 state_reference_unsupported/409/not_started，未支持操作为 unsupported_operation/501/not_started，非法请求为 invalid_input/400/not_started。目标未注册、撤销或鉴权失败沿用现有明确的 not_started 拒绝链。发送后超时、断流、非法上游体或无法确认结果必须 result_unknown/502/unknown，retryable=false。应用取消只记 cancelled_unknown，不保证有可写的客户端响应。原生 HTTP JSON 错误直接保留上游状态与安全正文，不能将其伪装成本地合同错误；HTTP 4xx 诊断 failed，其余非成功 unknown；SSE error/response.failed 为 failed，response.incomplete 为 unknown。所有情况均无自动重放。

新旧包不能按请求字段猜测选路：入口/合同标识与配置版本空间必须同时明确；不支持的新版本在发送前拒绝，不能退回旧 Chat/latest。平台生产者与网关消费者必须通过同一个正式包的联合实例、不可变版本/撤销/过期和主体负例，之后才由协调者安排公开入口。候选 schema 有意只是字段边界，不代替这些语义验收。
