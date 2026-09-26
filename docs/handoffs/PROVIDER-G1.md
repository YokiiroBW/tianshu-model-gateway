# PROVIDER-G1：受控 OpenAI 兼容执行模块

基线 `601974194042641c5a85cc3c061cbd1880d7daf1`，分支 `codex/provider-gateway-20260926`。本交付为独立模块，尚未接入管理 HTTP、现有对话路由或 NAS。没有真实模型/账号/付费调用，没有修改旧 ClientGrant、依赖锁、共享合同或其他产品。

## 实现

`src/tianshu_gateway/provider_adapter.py`：

- 私有 frozen `ExecutionContext(provider_id, revision, base_url, api_key, model_id, protocol)`。与 PROVIDER-P1 字段对齐；地址、模型和密钥不出现在 repr，模块没有持久化、日志或数据库。该 DTO **不是授权证明**，不得从浏览器/未认证 RPC 直接构造后执行。
- `OpenAIAdapter.models(context)` 实际 GET `<base>/models`，只返回有界、去重的模型 ID tuple；不表示模型生成成功。`test_reply(context)` 固定发送 “Reply with OK.”、max_tokens=16，只有有效非空文本 completion 才返回绑定 provider/revision/model 的 `reply_verified`。
- `complete(context, payload)` 是显式注入目标/凭据的非流式文本执行接点，固定所选模型，不允许请求改 model 或启用 stream；不是现有普通运行路由替换品。完整 SSE、tool calls、现有用量/回执/持久观察逻辑仍由原 server 拥有，后续集成应复用原路径。
- API Key 无效、模型枚举不支持、端点不存在、明确机器码 model_not_found、限流、重定向、无效响应分别给固定原因；不回传供应商错误正文/响应头。未知网络结果、timeout、cancel 不重试；取消向调用者传播 CancelledError，不能据此宣布供应商未执行。
- 默认只允许公共 HTTPS：拒绝内部/loopback/link-local/metadata/保留/multicast/IPv4映射/过渡IPv6目标。局域网必须显式 `connection_type='local'`，并命中部署注入的 RFC1918/ULA CIDR；浏览器不能决定 CIDR 或 TLS context。loopback 即便填进部署 CIDR 仍拒绝。HTTP 仅限此显式局域网模式。
- DNS 在总超时内逐请求解析一次，全部地址合规才发送，RegisteredTargets 固定本次所有 socket 的地址，保留主机名 TLS 校验。禁 redirect、环境代理、cookie、连接复用和 aiohttp 隐式幂等重试。TLS 必须校验证书/主机名；有界请求与响应；复用 read_limited、SecretGuard，另检查解码后的 JSON 字符串/键，阻止 Unicode escape 或分片凭据反射。

## 精确运行接点建议（交协调者冻结合同后实施）

当前 `server.py::Gateway.authenticate` 只校验静态调用者；Chat 选定 version 后经 `ConfigCache.get`、`routing.prepare`，在 chat 入口约 786 行从 EnvSecrets 取供应商 key、RegisteredTargets 校验 base。排队后 `requeue_reverify` 再取同 provider key；`forward` 使用启动共享 session。只替换最初一次 secret.resolve 不足以完成动态运行。

建议新增独立、默认关闭的受信 runtime source，输入服务身份、workload、精确 config_version/provider_id；输出短期授权/撤销信息及同版本 provider revision 的私有执行上下文。服务认证、snapshot 版本绑定与 provider/revision 要联合校验；不可扩宽 allowed_versions 为全部，不可把最新供应商上下文替换在途版本。排队后重新验证授权/撤销/租期，再取得同版本 credential/target；运行请求仍经过原 scheduler、持久 begin_upstream/diagnostics、回执与 SecretGuard，并将本次动态 key 纳入其 secrets 集合。动态 socket 必须使用本模块相同目标策略/逐次固定 resolver，不能继续使用启动时静态 session 的 resolver。

管理 `models/test_reply` 需要独立管理员操作授权与平台 → 网关认证 RPC；管理员测试 context 不能成为普通生成授权。平台独占目录和密钥；网关不读取平台 DB。平台须把测试结果以 expected revision CAS 写回，枚举成功不写测试成功。停用/编辑/撤销到动态授权链的失效期限、版本旧密钥可否继续使用、租期续期仍须合同规定。本模块没有自行选择这些跨产品语义。

## 实际验证

解释器：根 `.runtime/nas-a1-r1-venv/Scripts/python.exe`，aiohttp **3.14.1** 与实际项目锁一致；ruff 实际 **0.15.6**（manifest dev 要求 0.15.7，未修改依赖环境/锁）。设置 `PYTHONPATH=src`、`TIANSHU_WORKSPACE=C:/YOKI/Codex/tianshu-peiban-bot`，执行：

```
python -B -m unittest discover -s tests -p test_provider_adapter.py -v
python -m ruff check src/tianshu_gateway/provider_adapter.py tests/test_provider_adapter.py
python -m ruff format --check src/tianshu_gateway/provider_adapter.py tests/test_provider_adapter.py
git diff --check
```

17 项通过：真实本地 TLS 枚举/短回复及 5 key 并发隔离、真实 HTTP 自定义 DNS 固定、混合 DNS 拒绝、生产 loopback 拒绝、未信任证书拒绝、redirect 不跟随、401/403/404/405/429/500 去敏分型、端点与明确模型错误区分、空回复拒绝、分片/Unicode/含引号 key 反射阻断、有界响应、DNS 总超时、上游超时/取消/断连不重发、model 不可改写。HTTP/TLS 服务都是隔离录制替身，FixtureTargets 仅在测试中放行 loopback；没有访问公网供应商。新模块未被旧 server import，因此没有重复旧未变化全套测试。

未验证：平台真实 RPC 生产消费、HTTP 管理路由、管理员网页、动态 runtime grant/撤销/续期、SSE 动态目标接线、容器/NAS/真实供应商/真正对话。不能据本记录宣称自助供应商整体完成或部署完成。

## 2026-09-26 后端接线续交

本工作树现有独立管理 `models/test` HTTP 路由、专用平台凭据、动态 runtime source 及真实 Chat 主链路接线。每次动态请求在入队前、出队后和建连后向平台读取同一精确版本/turn 授权，socket 目标逐请求 DNS 固定，执行继续走原 scheduler、诊断、SSE 和回执。原文“尚未接入 HTTP/主链路”只描述第一次 G1 交付。网关不读取平台目录或跨产品数据库。

验证：适配器 17 项、现有 HTTP 28 项、根平台-网关-陪伴联合 4 项通过；全部为隔离本地 HTTP/TLS 录制，不含真实供应商。当前没有 NAS/容器或付费调用。完整证据和部署接线见根 `docs/handoffs/PROVIDER-BACKEND-2026-09-26.md`。

### 审查修复续交

`TargetPolicy` 现识别 RFC 6052 well-known NAT64 /96 前缀中嵌入的 IPv4，并对该地址重复执行原目标策略；`provider_nat64_prefixes` 允许部署登记实际使用的网络专用 /32、/40、/48、/56、/64、/96 前缀，启动时校验格式。仅 DNS/应用代码无法推断未登记的翻译路由，部署仍须限制出站目标。原公网 HTTPS/TLS 主机名校验未放宽。适配器 18 项、现有 HTTP 28 项、根联合 6 项通过；仍未访问 NAS 或真实供应商。
