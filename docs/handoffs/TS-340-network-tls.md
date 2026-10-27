# 统一宿主外部模型 TLS 上下文

基于 TS-337 `b7636079a0545ed8bc039407b3def5fd485e385a`，补充原 `create_app(..., network_ssl=None)` 可选显式 TLS context，交给原真实外部连接池；默认保留原 aiohttp 行为。统一宿主在设置 Platform 全局环境前，从 Gateway 自己的 `SSL_CERT_FILE` / `SSL_CERT_DIR` 构造 context，避免 aiohttp 导入时缓存的全局默认上下文忽略 Gateway 独立 CA。没有关闭证书或主机名检查；Platform 本地 source session 仍独立。

复用原观测测试 TLS 证书和真实 HTTPS loopback provider，原 Gateway 授权、Platform snapshot、本地 source transport 及原模型 response 合同通过。`tests/test_internal_clients.py` 集中 **4 passed / 0.69s**；新增一项是真实 HTTPS 握手与返回，未调用真实模型。

首次尝试共享发行环境缺 pytest，只是验证命令不可执行；随后复用既有 Companion q21 Python + Platform q21 dependencies、显式源码路径及冻结 group 合同运行通过。未重跑未变全量、未部署。
