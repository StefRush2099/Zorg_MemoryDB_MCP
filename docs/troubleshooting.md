# Troubleshooting

## Stack won't start

- **`dockge_default` network not found**: Run `docker network create dockge_default`.
- **DB not ready**: Check `docker logs db` — wait for "ready to accept connections". Then `docker compose up -d --force-recreate` for the other services.
- **Port conflict**: Check if 5432, 8080, 7779, 1781, or 1782 is already in use. Adjust ports in `compose.yaml`.

## MemoryDB tools not responding

- **SSE connection dropped**: Restart `postgres-mcp`: `docker restart postgres-mcp`.
- **Auth token mismatch**: Verify `POSTGRES_NATIVE_TOKEN` in `.env` matches what the plugin expects.
- **DB connection refused**: Check that `db` is running and `DATABASE_URI` is correct in `compose.yaml`.

## Context window overflow (request exceeds context size)

- **Symptom**: `request (N tokens) exceeds the available context size`
- **Cause**: Total per-turn payload (system prompt + tool definitions + history + recall) exceeds the llama.cpp `--ctx-size` launch flag.
- **Fix (short-term)**: Reduce history blocks and recall limit in `memorydb_recall_context`.
- **Fix (durable)**: Increase `--ctx-size` on the llama.cpp server. The model supports its trained context (check `n_ctx_train` in the model metadata). Restart the server with the new flag.

## Slot utilization stuck high

- **Symptom**: Slot not erasing, utilization stays above 70%.
- **Cause**: Model name mismatch in slot state query. The llama.cpp `/slots` endpoint exposes `n_prompt_tokens` only for the speculative model ID (base name + `--MTP` suffix). Using the base name alone returns no token field, so the size gate sees 0 tokens and never fires.
- **Fix**: Ensure the slot state query appends `--MTP` to the model name if absent.

## Recovery worker timeout

- **Symptom**: `memorydb_recovery_post` times out or returns errors after a crash.
- **Cause**: Transient DB outage during stack restart — the recovery worker fires before the DB is fully ready.
- **Fix**: Usually self-resolves within 1-2 minutes. If persistent, restart the stack in order: `db` first, then `postgres-mcp`, then `memorydb-plugin`.

## Heartbeat / crash detection

- Each component (`postgres-mcp`, `memorydb-openapi`, `memorydb-plugin`) writes a heartbeat via `memorydb_heartbeat_post` at startup and periodically.
- If a component is down for > 24h, its heartbeat rows will be stale. Check with:
  ```sql
  SELECT component, last_heartbeat_at FROM memory_component_heartbeats ORDER BY last_heartbeat_at DESC;
  ```
