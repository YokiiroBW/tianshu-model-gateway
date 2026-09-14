# model-protocol/v1 · 1.0.0 发布候选

状态：`release_ready_unpublished`。此目录供协调者审查、复制并发布，不能作为已发布合同或已启用接口的证明。根 `contracts/` 仍是唯一发布位置；历史 `docs/candidates/model-protocol-v1/` 不变，历史字段不能与本包混用。

本包只定义原生 `openai-responses` 的模型配置、可信路由上下文、运行回执与错误。旧 `text-dialogue/v1` Chat Completions 配置、协议标签、端口和版本不变。原生请求保留模型、instructions、input、tools、reasoning、store 及未知扩展字段；不转换为 Chat 消息。首轮只允许 `preserve_client`、`fallback=disabled`、`state_references=reject`。

## 包内容与独立验证

- `schemas/model.json`：新 wire 的有限形状边界；业务关系与鉴权要求见 semantics.md。
- 正反例、验证器及 manifest：由包维护者提供；成功验证仅证明离线合同，不证明运行时开放或真实模型能力。
- `interfaces.json`：明确平台快照与未来网关接口；route_context 仅内部投影。
- `semantics.md`：配置所有权、权限、参数保真、状态引用、观察和错误规则。
- `compatibility.md`：旧包隔离、发布和双方验收条件。

唯一外部 schema 依赖是 `text-dialogue/v1/common.json`，固定 SHA-256：

`b296a79d7eb0218d9b444c4ddbba837ad576f46a7a4e50fbcebec7618d8ef9af`

验证器使用必填 `--common` 指向显式离线文件（通常是已发布根的 `text-dialogue/v1/schemas/common.json`）；读取后先核对上述摘要。候选目录本身不是可信依赖发现入口。不得依赖产品源码、`.runtime/workspace-context.json`、某个任务 worktree、当前工作目录猜测或网络下载。缺文件、摘要不符或引用无法解析时明确失败；不得回退到其他 common 版本。

## 路径与版本

平台唯一 `Models` owner 提供独立 `POST /internal/v1/model-config/native/snapshot`。原生配置使用独立持久化表、递增版本与撤销链；所有新 wire 字段使用 `native_config_version`，不用旧 `config_version`。服务权限字段为 `caller.native_config_versions`，没有配置时按空集合处理；不得继承旧 `caller.config_versions`。

未来网关入口为 `POST /v1/responses`，诊断读取为 `GET /internal/v1/native-model-requests/{request_id}`。它们在接口目录中的状态均为 `runtime_disabled_until_joint_acceptance`，包括候选中的平台端口；接口存在于文档不等于服务已注册。route_context 由网关可信身份生成，不是第四个 HTTP 入口。

## 发布条件

协调发布不可变合同后，平台生产者和网关消费者必须共同验证快照权限/撤销/到期/版本，以及真实本地 HTTP 录制替身的原生 JSON、SSE 字节、错误、取消、未知用量和旧 Chat 回归，再由协调明确开放接口。离线验证、平台/网关替身联合验证与授权真实测试账号验证分开记录；无真实密钥、真实模型费用或生产操作是本包的交付要求。

官方协议依据：2026-09-14 实际打开核对 [OpenAI Responses 创建文档](https://developers.openai.com/api/reference/python/resources/responses/methods/create)。天枢的状态拒绝、配置权限和发布门槛属于本系统决策，不代表 OpenAI 的全部协议限制。


验证环境为 Python 3.12+、jsonschema==4.26.0、referencing==0.37.0（见 requirements.txt），不需要安装任何产品。执行：

```text
python -I -B validate.py --common /absolute/path/to/published/common.json
python -I -B test_validator.py --common /absolute/path/to/published/common.json
python -I -B validate.py --common /absolute/path/to/published/common.json --kind native_request --document /absolute/path/request.json
```

manifest 的 sha256 覆盖包内所有交付文件（manifest 自身除外），文本统一按 UTF-8/CRLF→LF 计摘要；验证器不写文件、不下载 schema、不自动更新哈希。发布者还须在包外固定 manifest SHA-256，防止文件与清单被一并替换。只有固定 common 是跨合同依赖；requirements.txt 是验证器运行依赖，不是新产品配置。

`trusted_access` 是认证适配器的内部验证输入：native_config_versions 对应 caller.native_config_versions，config_versions 仅在对照夹具中出现并被忽略。它不属于任何 HTTP request schema；复制字段或把 revoked 写为 false 不构成可信身份。validator 不实现认证、TLS、数据库、网关转发或生产权限服务。


关系样例采用一个完整 base 加具名 cases；每个 changes 条目只设定或移除明确路径的夹具值，路径数组中的整数表示列表位置。validate.py 展开后逐例检查精确预期错误；该格式只用于离线样例，不是配置更新 API。test_validator.py 另在临时目录复制包与 common，以 Python 隔离模式验证移植、CRLF、摘要篡改、额外文件及远程 schema 拒绝。
