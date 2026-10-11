# Memory 嵌入与重排序执行

新增可选部署字段 `memory_retrieval_credential_ref`，引用 `secret_references` 中独立的 Memory→Gateway 凭据环境变量。不可与聊天调用者、平台回读或 provider management 复用同一个引用；旧配置不启用新路由。

Memory 使用 `POST /internal/v1/memory-retrieval/execute`，Bearer 鉴权：

- `operation=settings`：通过既有登记目标与平台服务凭据读取策略，无记忆正文。
- `operation=embed, revision, texts`：最多 32 条，Memory 实际每批 4 条。调用兼容 `/embeddings`，强制 `encoding_format=float`，按返回 index 复原顺序。
- `operation=rerank, revision, query, documents`：最多 100 条，调用 `/rerank`，要求全部文档的 index 与有限 relevance_score，按输入顺序输出 scores。

网关向平台读取同 revision 的精确连接；请求完成后再次检查策略 revision，不跨模型空间拼接响应。平台 Gateway principal 需增加 `memory.retrieval` action。复用 `OpenAIAdapter` 的 DNS/目标规则、TLS 校验、禁重定向、响应体限制和无重试传输。实际模型正文只在 Memory 与网关、网关与上游之间传递。

私有 provider-self-service 测试通道另增 `embed` / `rerank` operation，在原 connection body 上增加 `payload`，供网页测试固定合成短句。它仍只接受既有 Platform 专用凭据。

嵌入与重排序共用 4 个执行槽；记录沿用 observed 路由日志，日志不包含记忆、密钥或模型返回正文。上游 usage 存在时随响应返回，缺失为 null；未新增账单聚合。默认只支持既有 public HTTPS 目标策略，不新增本地 HTTP 模型绕过路径。

参考协议：[嵌入](https://api-docs.siliconflow.cn/docs/api/embeddings-post)、[重排序](https://docs.siliconflow.cn/docs/api/rerank-post)。测试全部使用明确合成模型，不代表真实供应商召回质量成绩。
