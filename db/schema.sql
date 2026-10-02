-- Zorg MemoryDB MCP - Core Schema
-- This is the minimal schema for a fresh install.
-- The production schema (303+ tables, 328+ functions) is built incrementally.
-- For a full production schema, run pg_dump on a live instance.

-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS plpgsql;

-- Core memory table
CREATE TABLE IF NOT EXISTS zorg_memory (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_session_log text NOT NULL DEFAULT '',
    logged_at timestamptz NOT NULL DEFAULT now(),
    system_prompt text,
    ai_response text,
    ai_response_updated_at timestamptz,
    memory_key text NOT NULL,
    memory_value text NOT NULL,
    memory_effective_date date,
    memory_category text,
    memory_priority text DEFAULT 'normal',
    memory_active boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_zorg_memory_key ON zorg_memory (memory_key);
CREATE INDEX IF NOT EXISTS idx_zorg_memory_active ON zorg_memory (memory_active) WHERE memory_active;
CREATE INDEX IF NOT EXISTS idx_zorg_memory_logged_at ON zorg_memory (logged_at DESC);

-- Chat messages
CREATE TABLE IF NOT EXISTS lan_chat_messages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_key text NOT NULL DEFAULT 'agent:main:lan-chat',
    role text NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lan_chat_session ON lan_chat_messages (session_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_lan_chat_role ON lan_chat_messages (session_key, role, created_at DESC);

-- Logic rules
CREATE TABLE IF NOT EXISTS zorg_logic_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_key text NOT NULL UNIQUE,
    title text NOT NULL,
    rule_text text NOT NULL,
    rule_type text NOT NULL DEFAULT 'operator_rule',
    priority text NOT NULL DEFAULT 'normal',
    privacy_scope text NOT NULL DEFAULT 'public',
    source_basis text,
    applies_to text[] DEFAULT '{}',
    standard_checks text[] DEFAULT '{}',
    performance_tuning_notes text,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_zorg_logic_rules_active ON zorg_logic_rules (active) WHERE active;
CREATE INDEX IF NOT EXISTS idx_zorg_logic_rules_priority ON zorg_logic_rules (priority, active);

-- Runbooks
CREATE TABLE IF NOT EXISTS memory_runbooks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    runbook_key text NOT NULL UNIQUE,
    title text NOT NULL,
    scope text,
    trigger_text text,
    procedure_text text NOT NULL,
    source_path text,
    source_line_start integer,
    source_line_end integer,
    tags text[] DEFAULT '{}',
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_memory_runbooks_active ON memory_runbooks (active) WHERE active;

-- LLM registry
CREATE TABLE IF NOT EXISTS llm_registry (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    serving_system text NOT NULL,
    endpoint text NOT NULL,
    model_name text NOT NULL,
    context_window integer NOT NULL DEFAULT 131072,
    max_output_tokens integer NOT NULL DEFAULT 8192,
    supports_per_request_ctx boolean NOT NULL DEFAULT false,
    api_format text NOT NULL DEFAULT 'openai_compatible',
    auth_token text,
    is_active boolean NOT NULL DEFAULT true,
    last_probed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    probe_data jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_registry_endpoint_model ON llm_registry (endpoint, model_name);
CREATE INDEX IF NOT EXISTS idx_llm_registry_active ON llm_registry (is_active) WHERE is_active;

-- Compaction config
CREATE TABLE IF NOT EXISTS context_compaction_config (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    priority integer NOT NULL DEFAULT 0,
    endpoint text NOT NULL,
    model_name text NOT NULL,
    serving_system text NOT NULL DEFAULT 'ollama',
    context_window integer NOT NULL DEFAULT 131072,
    num_ctx integer NOT NULL DEFAULT 0,
    is_active boolean NOT NULL DEFAULT true,
    last_healthy_at timestamptz,
    failure_count integer NOT NULL DEFAULT 0,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_compaction_active ON context_compaction_config (is_active, priority) WHERE is_active;

-- Slot state
CREATE TABLE IF NOT EXISTS context_slot_state (
    session_key text NOT NULL DEFAULT 'agent:main:lan-chat',
    llm_endpoint text NOT NULL,
    model_name text,
    n_ctx integer,
    n_prompt_tokens integer,
    n_prompt_tokens_processed integer,
    last_erased_at timestamptz,
    last_saved_at timestamptz,
    save_filename text,
    context_items jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_key, llm_endpoint)
);

-- Component heartbeats
CREATE TABLE IF NOT EXISTS md_component_heartbeat (
    component text NOT NULL,
    host text,
    pid integer,
    started_at timestamptz,
    last_seen_at timestamptz,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (component)
);

-- Turn manifests
CREATE TABLE IF NOT EXISTS memory_turn_manifests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    turn_id text NOT NULL,
    transaction_id uuid,
    source_channel text,
    source_message_id text,
    expected_event_count bigint,
    captured_event_count bigint,
    gap_count bigint,
    complete boolean,
    started_at timestamptz,
    completed_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_turn_manifests_turn ON memory_turn_manifests (turn_id);

-- Tool calls
CREATE TABLE IF NOT EXISTS memory_tool_calls (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    turn_id text NOT NULL,
    transaction_id uuid,
    tool_name text NOT NULL,
    arguments jsonb NOT NULL DEFAULT '{}'::jsonb,
    arguments_hash text,
    status text,
    started_at timestamptz,
    finished_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tool_calls_turn ON memory_tool_calls (turn_id, created_at);

-- LLM job queue
CREATE TABLE IF NOT EXISTS memory_llm_job_queue (
    queue_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_key text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    due_at timestamptz,
    payload_snapshot jsonb,
    delivery_snapshot jsonb,
    leased_by text,
    leased_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    result_summary text,
    stdout_text text,
    stderr_text text,
    error_text text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_job_status ON memory_llm_job_queue (status, due_at);

-- Context notes
CREATE TABLE IF NOT EXISTS memory_context_notes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    note_key text NOT NULL,
    note_type text NOT NULL,
    title text,
    note_text text,
    source_kind text,
    source_path text,
    source_line_start integer,
    source_line_end integer,
    content_hash text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    active boolean NOT NULL DEFAULT true,
    imported_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_context_notes_key ON memory_context_notes (note_key);

-- Operator corrections
CREATE TABLE IF NOT EXISTS memory_operator_corrections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    correction_key text NOT NULL UNIQUE,
    query_text text,
    failed_behavior text,
    corrected_behavior text,
    affected_rule_keys text[] DEFAULT '{}',
    request_context jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    applied_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Semantic queue (for ANN embeddings)
CREATE TABLE IF NOT EXISTS memory_ann_model_embeddings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type text NOT NULL,
    source_key text NOT NULL,
    embedding_provider text NOT NULL,
    embedding_model text NOT NULL,
    embedding_dim integer NOT NULL,
    embedding vector,
    content_hash text,
    content_text text,
    priority text,
    event_ts timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ann_embeddings_active ON memory_ann_model_embeddings (active) WHERE active;
CREATE INDEX IF NOT EXISTS idx_ann_embeddings_source ON memory_ann_model_embeddings (source_type, source_key);

-- Embedding model slots
CREATE TABLE IF NOT EXISTS memory_embedding_model_slots (
    slot_key text PRIMARY KEY,
    embedding_provider text NOT NULL,
    embedding_model text NOT NULL,
    embedding_dim integer NOT NULL,
    endpoint text,
    enabled boolean NOT NULL DEFAULT true,
    is_default boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Memory categories
CREATE TABLE IF NOT EXISTS memory_categories (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    category_key text NOT NULL UNIQUE,
    name text NOT NULL,
    description text,
    aliases text[] DEFAULT '{}',
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Prompt blueprint
CREATE TABLE IF NOT EXISTS zorg_prompt_blueprint (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    blueprint_key text NOT NULL,
    section_order integer NOT NULL,
    section_title text NOT NULL,
    template_text text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Intent category map
CREATE TABLE IF NOT EXISTS zorg_intent_category_map (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    intent_key text NOT NULL,
    categories text[] NOT NULL,
    default_tools text[] DEFAULT '{}',
    confidence_hint text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Success query index
CREATE TABLE IF NOT EXISTS zorg_success_query_index (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    query_text text NOT NULL,
    intent text,
    outcome_summary text,
    source_session text,
    completed_ok boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Tool catalog
CREATE TABLE IF NOT EXISTS zorg_tool_catalog (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tool_key text NOT NULL,
    category text,
    capability text,
    use_when text,
    avoid_when text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Turn heartbeat
CREATE TABLE IF NOT EXISTS zorg_turn_heartbeat (
    turn_id text NOT NULL,
    session_key text NOT NULL,
    component text NOT NULL,
    state text NOT NULL,
    llm_endpoint text,
    llm_model text,
    started_at timestamptz,
    committed_at timestamptz,
    recovered_at timestamptz,
    last_beat_at timestamptz,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (turn_id, session_key, component)
);

-- Recovery log
CREATE TABLE IF NOT EXISTS md_recovery_log (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_key text NOT NULL,
    uncommitted_user_msg_id uuid,
    recovered_at timestamptz,
    worker_endpoint text,
    worker_model text,
    outcome text,
    committed_assistant_msg_id uuid,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);

