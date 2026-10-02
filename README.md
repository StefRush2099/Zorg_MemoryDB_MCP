# Zorg MemoryDB MCP

PostgreSQL-backed durable memory and operating-rule system for LLM assistants, exposed as an MCP (Model Context Protocol) service stack via Docker Compose.

## What it does

- **Persistent memory**: Stores all conversation turns, operator facts, rules, corrections, and events in a PostgreSQL database (`ollama_memoryDB`).
- **Semantic + weighted recall**: ANN vector search (pgvector) + weighted keyword/trigram/recency scoring for deep recall.
- **Crash/reboot recovery**: Uncommitted turns are detected and resumed automatically after restart.
- **Context-window management**: Per-turn size gate reads the live llama.cpp slot, flushes open work to the DB, and erases the slot when utilization exceeds 70%, so work resumes in a clean window.
- **MCP transport**: Exposes PostgreSQL tools and MemoryDB-specific tools over SSE for any MCP-capable client.

## Stack components

| Container | Image | Port | Role |
|---|---|---|---|
| `db` | `postgres:latest` | 5432 | PostgreSQL 18 + pgvector + pg_cron + plpython3 |
| `adminer` | `adminer` | 8080 | Web-based DB admin UI |
| `postgres-mcp` | `crystaldba/postgres-mcp:latest` | 7779 | PostgreSQL MCP bridge (SSE) with custom `server.py` |
| `memorydb-openapi` | `ghcr.io/open-webui/mcpo:main` | 1781 | OpenAPI/MCP proxy to postgres-mcp |
| `memorydb-plugin` | `local/zorg-memorydb-plugin:1` | 1782 | OpenClaw tool-catalog HTTP backend + recovery worker |

## Quick start

```bash
# 1. Copy the env template and set your values
cp .env.example .env
# Edit .env: set POSTGRES_PASSWORD, POSTGRES_NATIVE_TOKEN

# 2. Build the plugin image (from the repo root)
docker build -t local/zorg-memorydb-plugin:1 ./memorydb-plugin-build

# 3. Ensure the external Docker network exists (create once)
docker network create dockge_default

# 4. Start the stack
docker compose up -d

# 5. Verify
curl -s http://localhost:7779/sse | head -5
```

## Database schema

The schema is large (328+ functions, 500k+ embedding rows in production). For a fresh install:

1. Start the stack with an empty DB.
2. Run `db/schema.sql` (provided in the `db/` directory) to create tables, functions, and indexes.
3. Seed core rules and runbooks as needed.

> **Note**: The production schema is generated incrementally. The `db/schema.sql` file contains the canonical DDL for a clean install.

## MCP tool surface

The stack exposes two groups of tools:

**PostgreSQL tools** (via `postgres-mcp`):
- `execute_sql`, `list_schemas`, `list_objects`, `get_object_details`
- `explain_query`, `analyze_db_health`, `get_top_queries`
- `analyze_workload_indexes`, `analyze_query_indexes`

**MemoryDB tools** (via `memorydb-plugin` / `server.py`):
- `memorydb_get_system_prompt` — compiled system prompt from live DB
- `memorydb_recall_context` — full context package (prompt + recall + history + size gate)
- `memorydb_query_memory` — semantic/ANN/weighted recall
- `memorydb_search_chat_history` — older chat-history search (cap 20)
- `memorydb_get_chat_history_blocks` — bounded recent blocks (cap 30)
- `memorydb_begin_turn` / `memorydb_stream_model_output` / `memorydb_commit_turn` — turn lifecycle
- `memorydb_slot_state` / `memorydb_slot_erase` / `memorydb_slot_save` / `memorydb_slot_restore` — llama.cpp slot management
- `memorydb_compaction_config` — compaction target management
- `memorydb_detect_llm_context` — LLM endpoint detection
- `memorydb_context_items` / `_add_item` / `_remove_item` — context tracking
- `memorydb_recovery_post` / `memorydb_heartbeat_post` — crash recovery & health

## Configuration

All secrets are read from `.env` (never committed). See `.env.example` for the template.

| Variable | Description |
|---|---|
| `POSTGRES_PASSWORD` | Database password (required) |
| `POSTGRES_USER` | Database user (default: `ollama_cpp_mcp`) |
| `POSTGRES_NATIVE_TOKEN` | Native MCP token for memorydb-plugin (required) |
| `POSTGRES_MCP_SLOT_ENDPOINT` | llama.cpp endpoint for slot management (default: `http://localhost:9001`) |
| `COMPACTION_ENDPOINT` | Compaction model endpoint (default: `http://localhost:11434/api/chat`) |

## Project layout

```
├── compose.yaml              # Docker Compose stack definition
├── .env.example              # Environment template (copy to .env)
├── .gitignore
├── LICENSE
├── README.md
├── server.py                 # postgres-mcp custom server (MCP tools + MemoryDB handlers)
├── memorydb-plugin.py        # OpenClaw plugin HTTP backend + recovery worker
├── db/
│   ├── pg_hba.conf           # PostgreSQL client authentication
│   └── schema.sql            # Canonical DDL for clean install
└── docs/
    ├── architecture.md       # System architecture overview
    ├── install.md            # Detailed installation guide
    └── troubleshooting.md    # Common issues and fixes
```

## License

MIT — see [LICENSE](LICENSE).
