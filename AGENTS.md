# 项目开发约定

遵循天枢主工作区的 V2 总稿、当前任务卡及已发布的 contracts 版本。任务 worktree 的 .runtime/workspace-context.json 记录主工作区与明确基线。

只在分配的目录内实施，不改其他项目或共享合同；公共入口、依赖锁和迁移主线单人负责。禁止把未配置服务显示成成功。

当前没有业务服务或应用构建命令。TS-040 仅有 tests 内离线协议实验：

- `python -B -m unittest discover -s tests -p test_protocol_lab.py -v`：Python 3.12+ 标准库目标夹具。
- `python -B -m unittest discover -s tests -v`：另运行只读旧源码行为核对，需旧仓库依赖 cryptography==50.0.1；位置从 `.runtime/workspace-context.json` 或 `LEGACY_ROUTING_ROOT` 读取。
- `git diff --check`：差异格式检查。

使用 `-B` 防止在只读 references 生成字节码；不得复制旧源码进入本项目。完整命令和本机解释器见 README。旧源码哈希改变必须重审，不自动更新基线。首个业务脚手架任务再从实际 manifest/脚本增加应用安装与构建命令。验证需区分组件、替身联合、真实外部接入；本地实验通过不是网关转发通过。

本地隔离开发和提交用于审查；不自动推送、部署或操作真实设备。交付短记录 docs/handoffs/<任务编号>.md，含实际变更、验证、风险及下一步。
