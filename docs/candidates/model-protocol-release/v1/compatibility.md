# 兼容与发布边界

`model-protocol/v1` 1.0.0 当前是 `release_ready_unpublished`。它是独立的 Responses 模型合同，不是对 text-dialogue/v1 1.0.0 的原地放宽。历史候选 `model-protocol-v1/` 仅供追溯，不能与此包字段混搭。

| 边界 | 旧 Chat | 新原生 Responses |
| --- | --- | --- |
| 合同 | text-dialogue/v1 1.0.0 | model-protocol/v1 1.0.0 |
| protocol | openai-chat-completions | openai-responses |
| workload | companion.text | native.responses |
| 版本字段 | config_version | native_config_version |
| 服务版本授权 | caller.config_versions | caller.native_config_versions |
| 配置读取 | /internal/v1/model-config/snapshot | /internal/v1/model-config/native/snapshot |
| 配置存储/版本/撤销 | 既有 Chat 表与序列 | 同一 Models owner 的独立 native 表与序列 |
| 默认策略 | 按旧已发布合同 | 仅 preserve_client，客户端显式 model |
| 状态引用 | 按旧范围 | reject |

旧包的文件与摘要、schema 常量、权限、快照/配置 UI 和旧路由全部不因新包发布改变；不得把 Responses 塞入旧 provider 或 route receipt 并打 Chat 标签。原生授权不从旧 config_versions 继承；两种版本即使整数相同也不表示同一配置或权限。latest 原生读取仅从调用者授权且有效的原生配置中选择。缺少新授权或新配置就是不可用，不能以 Chat 回退掩盖。

新 schema 仅依赖固定 SHA-256 的已发布 common.json，复用其基础身份/query 类型不等于继承旧模型配置定义或权限。独立离线验证只接受显式 --common 文件；不读取产品、任务检出或可变平台实现作为合同依赖。

## 可审查交付与启用顺序

1. 协调者审查 schema、语义、正反例、关系验证、接口目录及依赖摘要，发布到根 contracts/model-protocol/v1 并记录不可变文件摘要。候选状态本身不授权运行。
2. 平台在唯一 Models owner 下实现新快照适配、独立 native 存储/递增版本/撤销、显式 native_config_versions 与授权 latest 筛选；生产者只使用已发布包。
3. 网关使用同一已发布包消费快照，依据认证身份生成 route_context，实现原生传输/旁路观察与按主体授权的诊断读取。平台和网关分别报告已发布合同版本与摘要，不从候选目录加载。
4. 双方联合验证明确版本、null latest、权限为空、只有旧授权、无权最高版本、过期/撤销、摘要/绑定/协议不一致以及不可用路径；负例必须在发送模型请求前失败。
5. 实际本地 HTTP 录制替身验证原生模型/思考/工具/未知字段不变、JSON 和所有 SSE 拆分边界、HTTP 和流内错误、超时/客户端取消/断流、部分/未知/零用量、诊断隔离和不重放；同时跑受影响旧 Chat 回归。
6. 协调者确认以上联合验收后，才将相关接口从 runtime_disabled_until_joint_acceptance 改为受控开放。真实测试账号与生产启用另行记录，替身通过不声明供应商能力或生产可用。

新版本不承诺已有 Chat 客户端自动成为 Responses 客户端；调用者需要显式选择新合同与原生版本授权。将来加入服务器状态、默认/强制参数策略、后台模式或其他协议必须另行发布兼容性变更与双方验收，不能仅因未知 JSON 字段可透传便默认开放。


已有 TS-042 内部传输提交只实现协议传输能力，其本地诊断仍用旧通用 ledger 的 config_version 键，不能直接作为本包回执或 native 撤销存储。正式网关消费者必须增加明确 native 版本适配与独立 ledger/key-space（含 contract、principal、caller、namespace），禁止将 native 版本7当作 Chat 版本7传入共享撤销表；该接线须随平台增量独立实现和验收。无需因本候选修改已验证的内部代码。
