# Changelog

## v1.0.0 (2026-10-04)

First public release of the Zorg MemoryDB MCP stack.

### What's included

- `server.py` — PostgreSQL MCP bridge (SSE) with 28 custom MemoryDB tools
- `memorydb-plugin.py` — OpenClaw tool-catalog HTTP backend + crash/recovery worker
- `compose.yaml` — Docker Compose stack (postgres:latest + pgvector + pg_cron + plpython3, adminer, postgres-mcp, memorydb-openapi, memorydb-plugin)
- `db/schema.sql` — Canonical DDL for clean install (328+ functions, pgvector ANN, weighted recall)
- `memorydb-plugin-build/` — Dockerfile + build context for the plugin image
- `docs/` — Architecture, install guide, troubleshooting
- `.env.example` — Environment template

### Key features

- Persistent memory in PostgreSQL (ollama_memoryDB)
- Semantic + weighted recall (pgvector ANN + keyword/trigram/recency)
- Crash/reboot recovery (uncommitted turns auto-resumed)
- Context-window management (live slot read, flush-then-erase at 70% utilization)
- Compaction for oversized requests (bounded map-reduce with reworded-by marker)
- MCP transport over SSE for any MCP-capable client
