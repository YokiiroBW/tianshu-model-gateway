# OpenCode provider test repair — local candidate

Base: `e4f112f02d28a1ded134126feed33f15e003ec20`, verified by the coordinator as the installed gateway image source. The coordinator's single original request returned HTTP 400 `MissingSessionID`; the adapter previously collapsed this into a connection failure.

The existing provider adapter and native forwarding path now supply the honest `tianshu-model-gateway/0.1.0` user agent and a hashed stable session only for exact HTTPS OpenCode Zen/Go endpoints. Management tests use a separate provider-stable session. Companion supplies an opaque actor/conversation session through its authenticated internal request; routing metadata does not grant permissions or change provider selection. Missing runtime session fails with controlled 400 before execution is claimed.

HTTP refusals retain a fixed error code and private HTTP status / allowlisted `missing_session_id` reason. Upstream messages, bodies, keys and context are not returned. The short probe still uses one `Reply with OK.` request with the existing 256-token ceiling and no retries.

Verification: 24 adapter tests and 29 gateway HTTP tests passed, using local HTTP/TLS fixtures only; targeted Ruff format/check and `git diff --check` passed. The coordinator's four candidate-product joint tests also passed. Official integration requirements: [Go documentation](https://opencode.ai/docs/go/); endpoint support: [Zen documentation](https://opencode.ai/docs/zen/).

This is local verification, not deployment or proof of actual provider acceptance. The coordinator owns serial deployment and one post-deployment short adapter probe. No default change, QQ delivery, migration, dependency or protocol expansion occurred here.
