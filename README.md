# 模型运行网关

原生协议、请求转发、线路与用量。

当前为 V2 开发准备骨架，未实现业务服务。旧代码来源见主工作区 workspace.json。

本项目有独立 Git；协调检出不供并发写入，任务在主工作区 worktrees 中进行。工作目录上下文见 .runtime/workspace-context.json，或回到主工作区 docs/development/CURRENT.md。

首个任务负责核定实际依赖、安装和检查命令；当前没有可运行应用，不执行生产连接。
