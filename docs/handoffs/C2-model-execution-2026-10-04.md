# C2 模型执行交接

基线 `d1865da927bee8e266ac34bdc674dc36c1a513a8`；分支 `codex/companion-complete-gateway-20261004`。交付提交见该分支 HEAD。仅 Gateway 本地候选，未推送、构建镜像、部署或调用真实模型/QQ。

## 变更与复用

- 复用原生 `/v1/chat/completions`、`/v1/responses` JSON/SSE 与原认证、固定版本、调度和回执，不另建生成接口。保留工具分片和后续工具结果的原生正文；Responses 增加 inline input_image，未实现的 file/audio/server-state 仍明确拒绝。Gateway 不执行工具或访问图片地址。
- 新增两协议各自的能力查询与 owned request 取消接口，准确区别 verified/unverified/unsupported。未验证能力允许尝试，fixture_only 不冒称供应商已验证。显式不支持才在上游前拒绝。
- `execution.py` 只拥有本地活动任务与无内容进度；持久权威仍为原 requests/native_requests 行。增量计数、固定结束原因、取消及错误码复用同回执。重启保留计数/实际用量，遗留在途标 unknown/interrupted，不重放。
- 排队取消持久标 upstream_started:false；开始后的取消关闭连接且结果可未知。未知活动状态不推导为未执行，其他主体不能取消。普通拒绝仍不建上游回执。并发重复编号保持 400 invalid_input，持久账本冲突仍 409。
- Native 调用沿用已有 OpenCode 稳定不透明 session 和诚实 User-Agent，仅现有精确主机/路径集成规则生效，不改变授权。

正式合同由协调者发布至现有 text-dialogue/v1、model-protocol/v1。哈希仅由 `src/tianshu_gateway/contracts.py` 维护；本交付采用 text manifest `90697e6ecbb587d3db8c8e4682f7f8f43a8b1a98f70d8835c8282b03828d2d3a`、native manifest `832abdbfbbb49d71bffc0aabdd816f4de92d892680f26cc5f05402e397d34262`。没有新增依赖、数据库表/schema 迁移、配置目录或删除文件。

## 实际验证

用既有 gateway-provider-test venv，PYTHONPATH 指向本检出 src/tests，TIANSHU_WORKSPACE 指向协调检出正式合同。

- 集中运行 13 个相关测试模块，共 382 项：369 通过、2 失败、11 跳过、0 errors。失败均为调度夹具中的同一重复编号用例；恢复原 400 后定向复验两项通过，并新增固定公共 code 断言。其他已通过测试没有重复运行。
- 新 `test_model_execution_http.py` 13 项全部通过：真实 loopback HTTP，覆盖完整/一字节 SSE、增量/最终回执及实际用量、工具/图片及工具结果原文、明确能力失败与未验证可尝试、上游断流/超时、活动和排队取消、native 完整身份隔离、OpenCode 路由元数据、重启未知与不重试。
- 相关模块为 model_execution_http、gateway_http、gateway_boundaries、responses、responses_native、responses_candidate、usage_report、ts044_scheduling、provider_adapter、origin_renewal、origin_renewal_joint、observability、health。11 项 origin_renewal_joint 因未配置 TS110_PLATFORM_ROOT 跳过，未宣称真实 Platform 联合通过。
- 受影响 Python 文件 ruff check、format --check 与 git diff --check 通过。候选闭合扩展 schema 的 8 正/7 反样例通过；协调者另确认正式合同旧 Chat/native 样例与 portable validator 通过。

## 仍有影响的边界与下一步

供应商工具、视觉和流能力仅以隔离夹具验证协议透传，不代表真实 provider/model 能力。未进行镜像/NAS/真实供应商验收，消费者接线与发布由协调者推进。

流 forwarded_bytes 是 await write 接受的字节，不证明终端收到。非流 JSON 必须先写最终回执再交框架发送，未观察传输的计数保持 0。未知/部分用量不推算完整用量；取消不声称供应商停止执行或不收费。旧无 execution 回执兼容；严格旧 schema 消费者需随正式合同升级。

新增取消是当前单进程任务控制，同一 SQLite 行保留终态，不是持久执行队列或供应商取消 API。重启不续跑、不恢复队列，也不自动重试。
