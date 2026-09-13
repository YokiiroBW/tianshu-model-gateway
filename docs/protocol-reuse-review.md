# TS-040 模型原生协议与旧网关复用审查

2026-09-14。状态：供协调者审查；没有真实转发、共享合同冻结或生产接入。

## 结论与依据

旧请求规划器不能直接作为 V2 外部原生入口：它明确覆盖客户端模型与思考字段。可以选择性复用校验、协议路径和凭据隔离思想；状态黏性、真实流式传输、运行用量与向量空间约束需要新实现。首轮文字链候选采用 Chat Completions 原生透传，Responses/Anthropic 保留独立协议，不降成同一种消息结构。

权威依据是主工作区 `docs/architecture/tianshu-system-architecture-v2.md` §3.4、§4、§9、A14，以及 `docs/architecture/model-providers-and-workloads-design.md` 的参数、备用线路与向量规则。能力目录使用 **I14 配置 / I15 模型运行**，I05/I06 不是模型接口。合同只由 TS-001 在主工作区 `contracts/` 发布；以下实验字段不是第二份合同。

只读来源：主工作区 `references/legacy-routing`，Git `f73569edd14f7a92e7b8417344004b499f52554f`。审查源文件 SHA-256 见 `tests/fixtures/legacy-source.json`。此处“旧代码”专指该参考仓库 `src/aigateway`，不是 `legacy/release-reference`，也不是对旧线上服务的实测。

## 旧行为与取舍

以下路径相对上述只读参考仓库，行号按固定来源。

| 主题 | 已观察的旧行为与证据 | V2 取舍 |
| --- | --- | --- |
| 模型/思考 | `dataplane.py:88–113,183–196` 设置 `payload.model=route.model`，按三协议覆盖 effort；旧 `tests/test_dataplane.py` 明确测试覆盖成功 | 替换所有权规则：外部默认保留；只补缺省或强制映射都需显式版本化策略与执行记录 |
| 原生字段 | `dataplane.py:115–134` 经 JSON 深复制，未重建工具列表；嵌套扩展字段可保留 | 可复用语义复制思路；保真不等于字节级请求一致，不能用有限类型白名单丢未知 JSON 字段 |
| 请求边界 | `dataplane.py:136–180` 限体积、校验 stream 类型和控制字符、过滤客户端凭据、保留协议头 | 复用并单独验证边界；这是有限形状校验，不是完整供应商 schema 校验 |
| 协议入口 | `PROTOCOL_PATHS` 只有三种 POST 路径 | 三协议保持独立；嵌入、检索响应、compact 等路径在本次旧规划器中被拒绝，TS-041 明示支持范围 |
| 流式 | `UpstreamResponse` (`dataplane.py:72`) 是整块 bytes，`UpstreamTransport` (`:78`) 只是 Protocol；`forward` 调用注入 transport 一次 | 替换为实际流读取/取消/背压/超时处理，布尔 stream 与返回 bytes 不证明在线 SSE |
| 状态引用 | `repository.py:507` 仅按 client 当前 applied binding 选路；`dataplane.py:221` 每次重新取路线 | 引用本身被复制，但没有 response/conversation/file ID 到旧快照的解析；新增认证主体与上游命名空间黏性 |
| 回退 | `DataPlaneService.forward` 只有一次 send，没有兼容性回退逻辑 | 首轮禁回退；后续仅显式、兼容、有界且确定可重试时启用 |
| 用量 | send 的 response 原样返回，未见模型运行用量归一/计价逻辑 | 新增独立运行记录；缺失不填 0、价格未知不填免费、部分统计不冒充完整统计 |
| 嵌入 | `domain.py:17` 无 embedding 协议，路径表也无 `/v1/embeddings` | 新增专用入口前先固定集合空间并与记忆方验收，禁止走聊天默认模型回退 |
| 配置 | `BindingSpec` 强制 route/model/reasoning，通用 effort 枚举只有 low/medium/high/xhigh/max/ultra | 不复用成跨供应商真理；模型/接口能力分别确认，未知参数不静默改成枚举默认值 |
| 控制/凭据 | `http_server.py` 接 ControlApi；forward 重验 endpoint、解密后 finally wipe | 凭据隔离与重验可作参考；平台是配置权威，不能移植旧独立控制平面造成双写。端点策略需兼容 V2 明确配置的本地服务，不能直接照搬公网限制 |

此次没有运行旧完整安全/数据库/部署测试，也不据此为上述复用候选出具安全认证。

## 协议核验与夹具

2026-09-14 查阅官方资料，仅核对字段形状，不推断虚构模型支持情况：

- Responses 保留 `reasoning`、工具调用与 `previous_response_id`；后者关联先前响应，不能把它当可跨上游的业务会话编号。[Responses 创建文档](https://developers.openai.com/api/reference/python/resources/responses/methods/create)
- Chat Completions 保留 `reasoning_effort`、`tool_calls`/`tool_call_id` 与 `stream_options`，不转换成 Responses 的同名概念。[Chat 创建文档](https://developers.openai.com/api/reference/python/resources/chat/subresources/completions/methods/create)
- 原生流事件应保留事件类型及增量字段。[OpenAI 流式文档](https://developers.openai.com/api/docs/guides/streaming-responses)
- Anthropic 流有 `input_json_delta`、`signature_delta`、ping 和错误事件，不能仅抽取文本后宣称完整转发。[Claude 流式文档](https://platform.claude.com/docs/en/build-with-claude/streaming)

`native.json` 包含三类请求及三个合成 SSE **片段**。模型名、工具、状态 ID、签名全是隔离样例；不是可发送的真实能力探测，也不验证字段组合被任何商家接受。未知 `x_fixture_extension` 专门检验不丢字段。

| 可执行证据 | 验证内容 | 不证明的范围 |
| --- | --- | --- |
| `test_legacy_characterization.py` | 执行真实旧规划器，覆盖行为可重复；其余工具/扩展字段相等；协议头、非法值拒绝、缺失路径、固定源哈希 | 旧服务/生产转发、供应商能力 |
| `NativeFidelityTests` | 整体 JSON 深相等；保留/默认/显式映射；null/false/0 不当缺失；修改范围及变化记录 | 认证配置解析、正式路由决策、完整 API schema |
| SSE 分片与异常测试 | 每个双分片边界（含 UTF-8 内部）保持 bytes；惰性消费；上游异常不吞、不自动重放；删除字段/字节的负面对照 | HTTP/TLS、代理缓冲、网络背压、断线取消、SSE 完整性识别 |
| `RoutingBoundaryTests` | 引用锁住完整旧快照；跨主体/未知/冲突拒绝；兼容回退正反矩阵；未知/零/部分用量；嵌入六项身份差异拒绝 | 持久化、重启恢复、真实 token 计价、向量重建 |

实验采用保守回退：模型/参数相同才可能允许备用线路。显式换模型的兼容回退尚未实现，不能把测试绿色理解为它已受支持。`select_route` 接受已经分类的引用，原生 JSON 引用提取、过期/撤销/失效快照处理留给正式协议适配层。

## TS-041 接入前条件与后续验收

1. TS-001 的 I14/I15 候选需经协调者发布并确认双方使用版本；本实验的 `explicit_mapping` 对应候选 force 概念，名称不得直接当 wire 枚举使用。
2. 正式入口按已认证调用者解析工作负载/配置版本，拒绝伪造或过期版本；内部关联头不泄漏到上游。配置与请求身份不是模型正文的权威字段。
3. 外部保留精确模型与思考字段；内部绑定解析在请求开始前完成。若使用默认/强制，receipt 记录 requested/effective/config_version/策略来源。能力未验证与不支持分开；不得静默降低参数。
4. response/conversation/file 引用必须解析到 caller/provider/credential namespace/旧配置与模型，缺失、冲突、禁用路线要明确失败；重启后也不可默默走新默认。
5. 首轮回退 disabled。后续测试已发送超时、工具结果未知、已输出后错误、身份跨域与尝试耗尽；不能拼接两个上游的流。源异常应作为未知/失败记录，不能写成功终态。
6. 正式本地联合验收用录制替身检查真实 HTTP 收到的原生 JSON/协议头，以及错误状态、分片、取消、超时、请求大小和凭据不外泄。随后经授权的测试账号单独验证真实提供商；两类结果分别报告。
7. embedding 绑定至少区分 provider/model/revision/dimensions/preprocessing/space，读写一致；变更需新索引重建、覆盖与召回验收、切换及回退。未发布相应合同前关闭该能力，不能沿用聊天备用。

未完成项是 TS-041/后续明确范围，不是本次已实现服务。具体运行结果见交接文件。
