# Architecture

## Overview

Zorg MemoryDB MCP is a PostgreSQL-backed durable memory system for LLM assistants. It provides persistent storage, semantic recall, crash recovery, and context-window management — all exposed as MCP tools over SSE.

## Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Stack                      │
│                                                             │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │   db     │◄───│ postgres-mcp │───►│ memorydb-openapi │  │
│  │(postgres)│    │  (SSE:7779)  │    │   (proxy:1781)   │  │
│  └────┬─────┘    └──────┬───────┘    └──────────────────┘  │
│       │                 │                                   │
│       │          ┌──────┴───────┐                          │
│       │          │  memorydb-   │                          │
│       │          │  plugin:1782 │                          │
│       │          └──────────────┘                          │
│       │                                                    │
│  ┌────┴─────┐                                            │
│  │ adminer  │  (web UI: 8080)                             │
│  └──────────┘                                            │
└─────────────────────────────────────────────────────────────┘
```

## Data flow

1. **User request** arrives at the LLM assistant.
2. **Turn start**: `memorydb_begin_turn` persists the request to `lan_chat_messages`.
3. **System prompt compilation**: `memorydb_get_system_prompt` compiles the working prompt from live DB data (core rules, runbooks, intent detection).
4. **Recall**: `memorydb_recall_context` runs weighted + ANN recall, core-rule preflight, bounded chat history, and the size gate (reads live llama.cpp slot, auto-erases if >70%).
5. **During generation**: `memorydb_stream_model_output` captures exposed output chunks.
6. **Turn end**: `memorydb_commit_turn` commits the final response + metadata.
7. **Crash recovery**: On restart, `memorydb_recovery_post` detects uncommitted turns and resumes them.

## Memory model

- **Primary store**: `public.zorg_memory` — all persistent memory (facts, rules, corrections, events, session logs).
- **Supporting stores**: `memory_*` tables (embeddings, job queues, turn manifests, tool calls, context notes).
- **Chat history**: `lan_chat_messages` — bounded conversation blocks.
- **Logic rules**: `zorg_logic_rules` — operator-authorized rules with priority ordering.
- **Runbooks**: `memory_runbooks` — procedures including the system prompt protocol itself.

## Recall stack

Recall uses a weighted scoring system combining:
- **Timestamp/recency**: newer rows score higher
- **Token overlap**: keyword and trigram matching
- **Supersession**: newer effective rows override older ones
- **Priority**: critical > high > medium > normal > low
- **ANN vector search**: pgvector semantic similarity

## Context window management

The size gate runs every turn inside `memorydb_recall_context`:
1. Reads the live llama.cpp slot state (via `memorydb_slot_state`).
2. If utilization > 70%, performs **flush-then-erase**: persists open work to `memory_context_notes`, then erases the slot.
3. Work resumes purely from MemoryDB recall in a clean window.

The model ID must include the `--MTP` suffix for accurate `n_prompt_tokens` (base name alone returns no token field).
