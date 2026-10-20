# 可选 PostgreSQL / Redis

默认仍使用 `diagnostics_path` 指定的 SQLite。该路径同时是本地后端绑定文件的位置锚点，切换 PG 时不要填 URL 到这里，也不要更换状态目录。

在部署 JSON 添加下列块，或在创建容器时填写对应环境变量。非空环境变量优先。

```json
{
  "storage_backends": {
    "postgresql_url": "postgresql://gateway_user:password@postgres:5432/tianshu",
    "redis_url": "redis://redis:6379/0",
    "namespace": "tianshu"
  }
}
```

环境变量为 `TIANSHU_POSTGRES_URL`、`TIANSHU_REDIS_URL`、`TIANSHU_STORAGE_NAMESPACE`。URL 放私有配置或环境文件，不要提交真实凭据。

启动会测试 PG 的事务写读权限，自动初始化空 schema，或将当前 SQLite 的全部账本字段导入、逐表核对计数和内容摘要。请求/轮次、配置和撤销、native 身份、用量及准入指标使用同一服务的 `diagnostics` schema，现有 Chat/native 主键隔离不变。PG 使用 BIGINT 保存 64 位版本、时间戳和 token 数。没有消息正文、媒体文件或模型输入进入该账本。

首次连接不通且没有 PG 绑定时继续 SQLite，并报告 PG 不可用。开始 PG 接续后，丢失配置、断线或 schema/绑定缺失会报错，不回写旧 SQLite。保留原 SQLite 和 `.tianshu-storage-gateway.json`；备份时连同当前 PG dump 与配置一起保存。

PG 实例保留单 owner 会话锁。第二个运行时不能把第一个实例的请求标为中断；断线后的会话不会自动重放写入。重启重新取得 owner 后，未完成请求按原规则记为 unknown，终态和幂等记录保留。

Redis 独立检测和旁路。网关本批没有新增 Redis 业务缓存；原配置刷新、来源/撤销核验和本地调度仍由原模块负责。Redis 不保存授权、可靠队列、回执或额度。

连接测试不初始化业务库，状态查询和用量 CLI 只读当前后端：

```console
python -m tianshu_storage --config settings.json test
python -m tianshu_storage --config settings.json --service gateway status --state-directory /var/lib/tianshu
python -m tianshu_gateway usage-report --settings settings.json --service companion
```

镜像通过锁定的 `vendor/tianshu_storage-0.1.0-py3-none-any.whl` 安装共享实现；不依赖开发目录或未发布的 PyPI 包。
