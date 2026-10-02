# ruff: noqa: B008
import argparse
import asyncio
import json
import logging
import math
import os
import signal
import sys
import urllib.error
import urllib.request
from enum import Enum
from typing import Any
from typing import List
from typing import Literal
from typing import Union

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from pydantic import validate_call
from starlette.requests import Request
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn

from postgres_mcp.index.dta_calc import DatabaseTuningAdvisor

from .artifacts import ErrorResult
from .artifacts import ExplainPlanArtifact
from .database_health import DatabaseHealthTool
from .database_health import HealthType
from .explain import ExplainPlanTool
from .index.index_opt_base import MAX_NUM_INDEX_TUNING_QUERIES
from .index.llm_opt import LLMOptimizerTool
from .index.presentation import TextPresentation
from .sql import DbConnPool
from .sql import SafeSqlDriver
from .sql import SqlDriver
from .sql import check_hypopg_installation_status
from .sql import obfuscate_password
from .top_queries import TopQueriesCalc

# Initialize FastMCP with default settings
mcp = FastMCP("postgres-mcp")

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


MAX_MEMORYDB_CHAT_BLOCKS = 30
DEFAULT_CONTEXT_WINDOW_TOKENS = 131072
DEFAULT_CONTEXT_COMPACTION_URL = "http://localhost:11434/api/chat"
DEFAULT_CONTEXT_COMPACTION_MODEL = "minicpm-v4.6:latest"
DEFAULT_CONTEXT_COMPACTION_TARGETS = [
    ("http://localhost:11434/api/chat", "minicpm-v4.6:latest"),
    ("http://localhost:11434/api/chat", "minicpm-v4.6:latest"),
]
DEFAULT_MEMORYDB_SESSION_KEY = "agent:main:lan-chat"


HEARTBEAT_COMPONENT = "postgres-mcp"

async def emit_component_heartbeat(component: str, payload: dict | None = None,
                                   sql_driver=None) -> None:
    """Write a heartbeat row for a MemoryDB component; never raises."""
    try:
        if sql_driver is None:
            sql_driver = await get_sql_driver()
        payload = payload or {}
        payload["component"] = component
        payload["pid"] = getattr(__import__("os"), "getpid", lambda: None)()
        await sql_driver.execute_query(
            """
            INSERT INTO public.md_component_heartbeat
              (component, host, pid, started_at, last_seen_at, payload, updated_at)
            VALUES (%s, %s, %s, now(), now(), %s::jsonb, now())
            ON CONFLICT (component) DO UPDATE SET
              host = EXCLUDED.host,
              pid = EXCLUDED.pid,
              last_seen_at = now(),
              payload = EXCLUDED.payload,
              updated_at = now()
            """,
            [component,
             payload.get("host", ""),
             payload.get("pid"),
             _to_json_text({k: v for k, v in payload.items() if k != "component"})],
        )
    except Exception as e:
        logger.warning("heartbeat emit failed for %s: %s", component, e)


async def run_uncommitted_recovery(session_key: str = DEFAULT_MEMORYDB_SESSION_KEY,
                                  max_age_hours: int = 24,
                                  sql_driver=None,
                                  reason: str = "startup") -> dict:
    """Detect uncommitted work and call the last active LLM worker to complete it."""
    result: dict = {"ok": False, "reason": reason, "session_key": session_key}
    try:
        if sql_driver is None:
            sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            "SELECT public.zorg_detect_uncommitted_work(%s, %s) AS detection",
            [session_key, max_age_hours],
        )
        det = (_rows(rows) or [{}])[0].get("detection")
        if not det or not det.get("has_uncommitted"):
            result.update({"ok": True, "recovered": False,
                          "detection_reason": (det or {}).get("reason", "unknown")})
            return result
        user_content = det.get("user_content", "")
        history = det.get("recent_history", []) or []
        system_line = (
            "You are Zorg, a persistent assistant backed by a PostgreSQL memory DB. "
            "Your last turn did not complete because the system failed or rebooted before "
            "the assistant reply was stored. Using the recent conversation context, answer "
            "the user's last message concisely and completely. "
            "Do not mention this recovery unless asked."
        )
        messages = [{"role": "system", "content": system_line}]
        for h in history:
            if h.get("role") in ("user", "assistant"):
                messages.append({"role": h.get("role"), "content": h.get("content", "")})
        messages.append({"role": "user", "content": user_content})

        endpoint = det.get("worker_endpoint")
        model = det.get("worker_model")
        api_format = det.get("worker_api_format", "")
        openai_style = (api_format == "openai_compatible") or str(endpoint).rstrip("/").endswith("/v1")
        if openai_style:
            url = str(endpoint).rstrip("/")
            if not url.endswith("/v1/chat/completions"):
                url = url + "/v1/chat/completions"
            body = {"model": model, "messages": messages, "temperature": 0,
                    "max_tokens": 1024, "stream": False}
        else:
            url = str(endpoint).rstrip("/")
            if not url.endswith("/api/chat"):
                url = url + "/api/chat"
            body = {"model": model, "messages": messages, "stream": False,
                    "options": {"temperature": 0, "num_predict": 1024}}
        req = urllib.request.Request(
            url, data=_to_json_text(body).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        loop = asyncio.get_running_loop()
        def _do_call():
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        payload = await loop.run_in_executor(None, _do_call)
        if openai_style:
            content = str(payload["choices"][0]["message"].get("content") or "").strip()
        else:
            msg = payload.get("message") or {}
            content = str(msg.get("content") or msg.get("reasoning") or msg.get("thinking") or "").strip()
        if not content:
            result.update({"ok": True, "recovered": False, "outcome": "empty_worker_response"})
            return result
        meta = _metadata(None, kind="assistant_final", source="postgres_mcp_recovery",
                         recovered=True, worker_endpoint=endpoint, worker_model=model,
                         recovered_uncommitted_user_msg_id=det.get("uncommitted_user_msg_id"),
                         recovered_reason=reason)
        await sql_driver.execute_query(
            "INSERT INTO public.lan_chat_messages (session_key, role, content, metadata) "
            "VALUES (%s, 'assistant', %s, %s::jsonb)",
            [session_key, content, meta],
        )
        await sql_driver.execute_query(
            "INSERT INTO public.md_recovery_log "
            "(session_key, uncommitted_user_msg_id, recovered_at, worker_endpoint, worker_model, outcome, details) "
            "VALUES (%s, %s::uuid, now(), %s, %s, 'recovered_and_committed', %s::jsonb)",
            [session_key, det.get("uncommitted_user_msg_id"), endpoint, model,
             _to_json_text({"reason": reason, "user_content": user_content[:2000]})],
        )
        result.update({"ok": True, "recovered": True, "outcome": "recovered_and_committed",
                      "worker_endpoint": endpoint, "worker_model": model})
    except Exception as e:
        logger.error("uncommitted recovery failed: %s", e)
        result.update({"ok": False, "error": str(e)})
    return result


@mcp.tool(description="Recover uncommitted MemoryDB work: detect a user request that was never answered (e.g. after a crash/reboot) and call the last active LLM worker to complete it, storing the result.")
async def memorydb_recovery(
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    max_age_hours: int = Field(description="Only consider uncommitted requests newer than this many hours", default=24),
) -> ResponseType:
    """Run the uncommitted-work recovery pass and report the result."""
    result = await run_uncommitted_recovery(session_key=session_key,
                                            max_age_hours=max_age_hours,
                                            reason="on_demand_tool")
    return format_text_response(_to_json_text(result))


@mcp.tool(description="Write a heartbeat row for a MemoryDB component (postgres-mcp, memorydb-openapi, memorydb-plugin) used for crash/reboot recovery gating.")
async def memorydb_heartbeat(
    component: str = Field(description="Component name", default=HEARTBEAT_COMPONENT),
    payload: dict[str, Any] = Field(description="Optional heartbeat payload (host, pid, state, last worker, etc.)", default={}),
) -> ResponseType:
    """Record component liveness."""
    await emit_component_heartbeat(component, payload)
    return format_text_response(_to_json_text({"ok": True, "component": component}))
MEMORYDB_SYSTEM_TOOL_INSTRUCTIONS = """
MemoryDB MCP contract:
- Treat PostgreSQL/MemoryDB as the sole source of memory recall and chat history.
- Use memorydb_query_memory for follow-up memory questions during a turn.
- Use memorydb_get_chat_history_blocks for bounded recent chat blocks; never request more than 30 blocks.

Context Management (self-configuration):
- The zorg memoryDB is the context source (brain, not flat text). After receiving a message, the FIRST action is an in-context search (zorg_weighted_recall_context + zorg_core_rule_preflight_v1). What is returned should be ALL that is needed to respond in that turn.
- Stale/out-of-context data from unrelated requests is NOT included. The slot is erased before each turn to ensure clean context.
- Compaction targets are self-configurable via memorydb_compaction_config tool:
  * action=list to see current targets
  * action=add to register a new endpoint (if a model goes down, add a backup)
  * action=remove to disable a dead target
  * action=health to probe all targets and update health status
  * Targets are tried in priority order; failed ones are skipped automatically
- Slot management via MCP tools:
  * memorydb_slot_state to check utilization
  * memorydb_slot_erase to clear stale context
  * memorydb_slot_save / memorydb_slot_restore to persist/restore context
- Context items tracking:
  * memorydb_context_items to view what's in the context
  * memorydb_context_add_item to track what was loaded
  * memorydb_context_remove_item to surgically remove items by type
- If the compaction model is down, check memorydb_compaction_config action=health and add a fallback endpoint.
- The context_window and num_ctx values come from context_compaction_config (not hardcoded).
- Stream exposed model output back through memorydb_stream_model_output when the client can do so.
- Commit final assistant output through memorydb_commit_turn.
- Only capture model fields exposed by the connected model/server. Do not invent hidden reasoning.
""".strip()


def _json_default(value: Any) -> str:
    return str(value)


def _to_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _jsonable(value: Any) -> Any:
    return json.loads(_to_json_text(value))


def _rows(rows: Any) -> list[dict[str, Any]]:
    return [row.cells for row in rows] if rows else []


def _clamp_limit(value: int, minimum: int = 1, maximum: int = 50) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = minimum
    return max(minimum, min(maximum, parsed))


def _metadata(base: dict[str, Any] | None, **extra: Any) -> str:
    data = dict(base or {})
    data.update({key: value for key, value in extra.items() if value is not None})
    return _to_json_text(data)


def _token_estimate(text: str) -> int:
    return max(1, math.ceil(len(text or "") / 4))


# ============================================================
# LLM Endpoint Scanner - Dynamic Context Window Detection
# ============================================================

def _http_get_json(url: str, auth_token: str | None = None, timeout: int = 10) -> dict | None:
    req = urllib.request.Request(url)
    if auth_token:
        req.add_header('Authorization', f'Bearer {auth_token}')
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            json.JSONDecodeError, OSError, ValueError):
        return None


def _http_get_text(url: str, auth_token: str | None = None, timeout: int = 10) -> str | None:
    req = urllib.request.Request(url)
    if auth_token:
        req.add_header('Authorization', f'Bearer {auth_token}')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        return None


async def scan_llm_endpoint(endpoint: str, auth_token: str | None = None) -> dict:
    'Scan an LLM endpoint to detect serving system, context window, and API format.'
    endpoint = endpoint.rstrip('/')
    result = {
        'endpoint': endpoint, 'serving_system': 'unknown', 'api_format': 'unknown',
        'context_window': 4096, 'max_output_tokens': 4096,
        'supports_per_request_ctx': False, 'model_name': None, 'found': False,
    }
    probe_data = {}

    # Strategy 1: Ollama /api/tags
    resp = await asyncio.to_thread(_http_get_json, endpoint + '/api/tags', auth_token)
    if resp and 'models' in resp and isinstance(resp['models'], list) and resp['models']:
        details = resp['models'][0].get('details', {})
        result['serving_system'] = 'ollama'
        result['api_format'] = 'ollama_api'
        result['supports_per_request_ctx'] = True
        if 'context_length' in details:
            result['context_window'] = int(details.get('context_length', 4096))
        result['model_name'] = resp['models'][0].get('name')
        result['found'] = True
        probe_data = {'source': 'ollama_api_tags', 'model_count': len(resp['models'])}

    # Strategy 2: OpenAI-compatible /v1/models (llama.cpp, vLLM, LM Studio)
    if not result['found']:
        resp = await asyncio.to_thread(_http_get_json, endpoint + '/v1/models', auth_token)
        if resp and 'data' in resp and isinstance(resp['data'], list):
            result['serving_system'] = 'openai_compatible'
            result['api_format'] = 'openai_compatible'
            result['context_window'] = 131072
            result['found'] = True
            if resp['data']:
                result['model_name'] = resp['data'][0].get('id')
            model_id = result['model_name'] or ''
            if 'gguf' in model_id or '/' in model_id:
                result['serving_system'] = 'llama_cpp'
            probe_data = {'source': 'openai_v1_models', 'model_count': len(resp['data'])}

    # Strategy 3: Ollama /api/version fallback
    if not result['found']:
        resp = await asyncio.to_thread(_http_get_json, endpoint + '/api/version', auth_token)
        if resp and 'version' in resp:
            result['serving_system'] = 'ollama'
            result['api_format'] = 'ollama_api'
            result['supports_per_request_ctx'] = True
            result['found'] = True
            probe_data = {'source': 'ollama_api_version', 'version': resp.get('version')}

    # Strategy 4: Generic health check
    if not result['found']:
        resp = await asyncio.to_thread(_http_get_text, endpoint + '/health', auth_token)
        if resp is not None:
            result['found'] = True
            probe_data = {'source': 'health', 'response': resp[:100]}

    probe_data.update({
        'system': result['serving_system'],
        'api_format': result['api_format'],
        'context_window': result['context_window'],
        'model': result['model_name'],
    })

    # Upsert into llm_registry
    try:
        sql_driver = await get_sql_driver()
        await sql_driver.execute_query(
            'INSERT INTO public.llm_registry '
            '(serving_system, endpoint, model_name, context_window,'
            'max_output_tokens, supports_per_request_ctx, api_format,'
            'auth_token, is_active, probe_data, last_probed_at) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true, %s, now()) '
            'ON CONFLICT (endpoint) DO UPDATE SET '
            'serving_system = EXCLUDED.serving_system, model_name = EXCLUDED.model_name,'
            'context_window = EXCLUDED.context_window, max_output_tokens = EXCLUDED.max_output_tokens,'
            'supports_per_request_ctx = EXCLUDED.supports_per_request_ctx,'
            'api_format = EXCLUDED.api_format, auth_token = EXCLUDED.auth_token,'
            'probe_data = EXCLUDED.probe_data, last_probed_at = now(), updated_at = now()',
            [result['serving_system'], endpoint, result['model_name'],
             result['context_window'], result['max_output_tokens'],
             result['supports_per_request_ctx'], result['api_format'],
             auth_token, _to_json_text(probe_data)],
        )
    except Exception as e:
        result['registry_error'] = str(e)

    return result


async def get_llm_context_from_registry(endpoint: str) -> dict | None:
    'Look up an endpoint in llm_registry for context window info.'
    endpoint = endpoint.rstrip('/')
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            'SELECT serving_system, endpoint, model_name, context_window,'
            'max_output_tokens, supports_per_request_ctx, api_format, is_active, probe_data, last_probed_at'
            'FROM public.llm_registry WHERE endpoint = %s AND is_active = true',
            [endpoint],
        )
        data = _rows(rows)
        return data[0] if data else None
    except Exception:
        return None


async def get_default_llm_context() -> dict:
    'Get default LLM context from registry or hardcoded fallback.'
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            'SELECT serving_system, endpoint, model_name, context_window,'
            'max_output_tokens, supports_per_request_ctx, api_format '
            'FROM public.llm_registry WHERE is_active = true'
            'ORDER BY (serving_system = ' + chr(39) + 'llama_cpp' + chr(39) + ') DESC, last_probed_at DESC NULLS LAST LIMIT 1'
        )
        data = _rows(rows)
        if data:
            return data[0]
    except Exception:
        pass
    return {
        'serving_system': 'llama_cpp',
        'model_name': 'draft/Qwen3.8-27B-GSQ-RCO-IQ3_S-mtp',
        'endpoint': 'http://localhost:9001',
        'context_window': DEFAULT_CONTEXT_WINDOW_TOKENS,
        'supports_per_request_ctx': False,
        'api_format': 'openai_compatible',
    }


DEFAULT_LLM_ENDPOINTS = [
    ('http://localhost:9001', None),
    ('http://localhost:11434', None),
    ('http://localhost:11434', None),
]

class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


# Global variables
db_connection = DbConnPool()
current_access_mode = AccessMode.UNRESTRICTED
shutdown_in_progress = False


async def get_sql_driver() -> Union[SqlDriver, SafeSqlDriver]:
    """Get the appropriate SQL driver based on the current access mode."""
    base_driver = SqlDriver(conn=db_connection)

    if current_access_mode == AccessMode.RESTRICTED:
        logger.debug("Using SafeSqlDriver with restrictions (RESTRICTED mode)")
        return SafeSqlDriver(sql_driver=base_driver, timeout=30)  # 30 second timeout
    else:
        logger.debug("Using unrestricted SqlDriver (UNRESTRICTED mode)")
        return base_driver


def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


@mcp.tool(description="List all schemas in the database")
async def list_schemas() -> ResponseType:
    """List all schemas in the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
            """
        )
        schemas = [row.cells for row in rows] if rows else []
        return format_text_response(schemas)
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


@mcp.tool(description="List objects in a schema")
async def list_objects(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """List objects of a given type in a schema."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            table_type = "BASE TABLE" if object_type == "table" else "VIEW"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = {} AND table_type = {}
                ORDER BY table_name
                """,
                [schema_name, table_type],
            )
            objects = (
                [{"schema": row.cells["table_schema"], "name": row.cells["table_name"], "type": row.cells["table_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type
                FROM information_schema.sequences
                WHERE sequence_schema = {}
                ORDER BY sequence_name
                """,
                [schema_name],
            )
            objects = (
                [{"schema": row.cells["sequence_schema"], "name": row.cells["sequence_name"], "data_type": row.cells["data_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "extension":
            # Extensions are not schema-specific
            rows = await sql_driver.execute_query(
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                ORDER BY extname
                """
            )
            objects = (
                [{"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]} for row in rows]
                if rows
                else []
            )

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(objects)
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Show detailed information about a database object")
async def get_object_details(
    schema_name: str = Field(description="Schema name"),
    object_name: str = Field(description="Object name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """Get detailed information about a database object."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Get columns
            col_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = {} AND table_name = {}
                ORDER BY ordinal_position
                """,
                [schema_name, object_name],
            )
            columns = (
                [
                    {
                        "column": r.cells["column_name"],
                        "data_type": r.cells["data_type"],
                        "is_nullable": r.cells["is_nullable"],
                        "default": r.cells["column_default"],
                    }
                    for r in col_rows
                ]
                if col_rows
                else []
            )

            # Get constraints
            con_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = {} AND tc.table_name = {}
                """,
                [schema_name, object_name],
            )

            constraints = {}
            if con_rows:
                for row in con_rows:
                    cname = row.cells["constraint_name"]
                    ctype = row.cells["constraint_type"]
                    col = row.cells["column_name"]

                    if cname not in constraints:
                        constraints[cname] = {"type": ctype, "columns": []}
                    if col:
                        constraints[cname]["columns"].append(col)

            constraints_list = [{"name": name, **data} for name, data in constraints.items()]

            # Get indexes
            idx_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = {} AND tablename = {}
                """,
                [schema_name, object_name],
            )

            indexes = [{"name": r.cells["indexname"], "definition": r.cells["indexdef"]} for r in idx_rows] if idx_rows else []

            result = {
                "basic": {"schema": schema_name, "name": object_name, "type": object_type},
                "columns": columns,
                "constraints": constraints_list,
                "indexes": indexes,
            }

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type, start_value, increment
                FROM information_schema.sequences
                WHERE sequence_schema = {} AND sequence_name = {}
                """,
                [schema_name, object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {
                    "schema": row.cells["sequence_schema"],
                    "name": row.cells["sequence_name"],
                    "data_type": row.cells["data_type"],
                    "start_value": row.cells["start_value"],
                    "increment": row.cells["increment"],
                }
            else:
                result = {}

        elif object_type == "extension":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                WHERE extname = {}
                """,
                [object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]}
            else:
                result = {}

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting object details: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Explains the execution plan for a SQL query, showing how the database will execute it and provides detailed cost estimates.")
async def explain_query(
    sql: str = Field(description="SQL query to explain"),
    analyze: bool = Field(
        description="When True, actually runs the query to show real execution statistics instead of estimates. "
        "Takes longer but provides more accurate information.",
        default=False,
    ),
    hypothetical_indexes: list[dict[str, Any]] = Field(
        description="""A list of hypothetical indexes to simulate. Each index must be a dictionary with these keys:
    - 'table': The table name to add the index to (e.g., 'users')
    - 'columns': List of column names to include in the index (e.g., ['email'] or ['last_name', 'first_name'])
    - 'using': Optional index method (default: 'btree', other options include 'hash', 'gist', etc.)

Examples: [
    {"table": "users", "columns": ["email"], "using": "btree"},
    {"table": "orders", "columns": ["user_id", "created_at"]}
]
If there is no hypothetical index, you can pass an empty list.""",
        default=[],
    ),
) -> ResponseType:
    """
    Explains the execution plan for a SQL query.

    Args:
        sql: The SQL query to explain
        analyze: When True, actually runs the query for real statistics
        hypothetical_indexes: Optional list of indexes to simulate
    """
    try:
        sql_driver = await get_sql_driver()
        explain_tool = ExplainPlanTool(sql_driver=sql_driver)
        result: ExplainPlanArtifact | ErrorResult | None = None

        # If hypothetical indexes are specified, check for HypoPG extension
        if hypothetical_indexes and len(hypothetical_indexes) > 0:
            if analyze:
                return format_error_response("Cannot use analyze and hypothetical indexes together")
            try:
                # Use the common utility function to check if hypopg is installed
                (
                    is_hypopg_installed,
                    hypopg_message,
                ) = await check_hypopg_installation_status(sql_driver)

                # If hypopg is not installed, return the message
                if not is_hypopg_installed:
                    return format_text_response(hypopg_message)

                # HypoPG is installed, proceed with explaining with hypothetical indexes
                result = await explain_tool.explain_with_hypothetical_indexes(sql, hypothetical_indexes)
            except Exception:
                raise  # Re-raise the original exception
        elif analyze:
            try:
                # Use EXPLAIN ANALYZE
                result = await explain_tool.explain_analyze(sql)
            except Exception:
                raise  # Re-raise the original exception
        else:
            try:
                # Use basic EXPLAIN
                result = await explain_tool.explain(sql)
            except Exception:
                raise  # Re-raise the original exception

        if result and isinstance(result, ExplainPlanArtifact):
            return format_text_response(result.to_text())
        else:
            error_message = "Error processing explain plan"
            if isinstance(result, ErrorResult):
                error_message = result.to_text()
            return format_error_response(error_message)
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Return the MemoryDB-backed system prompt and MCP memory tool instructions for any MCP-capable model.")
async def memorydb_get_system_prompt(
    user_request: str = Field(description="Current user request or empty string", default=""),
    context: dict[str, Any] = Field(description="Optional JSON context for prompt compilation", default={}),
) -> ResponseType:
    """Return system prompt/operator context compiled by MemoryDB."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT compiled_prompt, detected_intent, matched_rule_keys, matched_tool_keys, matched_categories
            FROM public.zorg_compile_system_prompt(%s, %s::jsonb)
            """,
            [user_request, _to_json_text(context or {})],
        )
        compiled = _rows(rows)
        prompt = compiled[0].get("compiled_prompt", "") if compiled else ""
        result = {
            "system_prompt": (prompt + "\n\n" + MEMORYDB_SYSTEM_TOOL_INSTRUCTIONS).strip(),
            "memory_tool_contract": MEMORYDB_SYSTEM_TOOL_INSTRUCTIONS,
            "compile_result": compiled[0] if compiled else {},
            "source": "ollama_memoryDB.public.zorg_compile_system_prompt",
        }
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error compiling MemoryDB system prompt: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Start a MemoryDB-backed chat turn by storing the inbound user request in lan_chat_messages.")
async def memorydb_begin_turn(
    user_request: str = Field(description="Inbound user request to persist"),
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    metadata: dict[str, Any] = Field(description="Optional JSON metadata", default={}),
) -> ResponseType:
    """Persist the inbound user message as the first block for a turn."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            INSERT INTO public.lan_chat_messages(session_key, role, content, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id::text, session_key, role, created_at, metadata
            """,
            [session_key, "user", user_request, _metadata(metadata, kind="user_request", source="postgres_mcp")],
        )
        await emit_component_heartbeat(HEARTBEAT_COMPONENT, {"state": "turn_begin", "session_key": session_key})
        return format_text_response(_to_json_text({"stored": _rows(rows)[0] if rows else None}))
    except Exception as e:
        logger.error(f"Error beginning MemoryDB turn: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Run MemoryDB semantic/ANN/weighted recall for the model during a turn.")
async def memorydb_query_memory(
    query: str = Field(description="Memory query text"),
    limit: int = Field(description="Maximum recall rows", default=10),
    context: dict[str, Any] = Field(description="Optional JSON recall context", default={}),
) -> ResponseType:
    """Query MemoryDB recall using the canonical stored procedure."""
    try:
        sql_driver = await get_sql_driver()
        recall_limit = _clamp_limit(limit, 1, 50)
        rows = await sql_driver.execute_query(
            """
            SELECT source_type, source_id, path, line_start, line_end, priority, content,
                   recall_mode, rank, score, score_reason, metadata
            FROM public.memory_recall_v2(%s, %s, %s::jsonb)
            """,
            [query, recall_limit, _to_json_text(context or {})],
        )
        return format_text_response(_to_json_text({"query": query, "limit": recall_limit, "results": _rows(rows)}))
    except Exception as e:
        logger.error(f"Error querying MemoryDB recall: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Build a DB-backed context package: system prompt, semantic recall, and <=30 bounded chat-history blocks.")
async def memorydb_recall_context(
    user_request: str = Field(description="Current user request"),
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    recall_limit: int = Field(description="Semantic recall limit", default=10),
    max_history_blocks: int = Field(description="Maximum chat-history blocks; hard capped at 30", default=30),
    context_window_tokens: int = Field(description="Available model context window estimate", default=8192),
    context: dict[str, Any] = Field(description="Optional JSON context", default={}),
) -> ResponseType:
    """Return a ready-to-use context package for an MCP-capable model."""
    try:
        sql_driver = await get_sql_driver()
        # Brain-context: check llama.cpp slot and erase if needed before each turn
        try:
            _llm_info = await get_default_llm_context()
            if _llm_info and _llm_info.get('serving_system') == 'llama_cpp':
                _slot_endpoint = (_llm_info.get('endpoint') or '').rstrip('/')
                _slot_model = (_llm_info.get('model_name') or '').rstrip('/')
                # llama.cpp /slots exposes n_prompt_tokens only for the actual
                # speculative model id (base + '--MTP'); the registry stores the
                # base name, so without this the hook reads 0 tokens and never erases.
                if not _slot_model.endswith('--MTP'):
                    _slot_model += '--MTP'
                if _slot_endpoint and _slot_model:
                    _should_erase, _slot_info = await prepare_context_for_llm(
                        _slot_endpoint, _slot_model, session_key,
                        context_window_tokens, 512,
                    )
                    if _should_erase:
                        # Flush-then-erase: persist open work to DB before wiping the slot,
                        # so the model can resume from MemoryDB in a clean window.
                        logger.info('  [recall_context] Flush-then-erase: util>70%%, persisting open work to DB before erase')
                        try:
                            await sql_driver.execute_query(
                                "INSERT INTO public.memory_context_notes"
                                " (note_key, note_type, title, note_text, source_kind, active, imported_at)"
                                " VALUES ("
                                "  'context-flush:' || %s || '-' || to_char(now(), 'YYYYMMDDHH24MISS'),"
                                "  'context_flush',"
                                "  'Pre-erase context flush',"
                                "  'Slot erased at util>70%%. Open work: ' || ("
                                "     SELECT coalesce(jsonb_agg(jsonb_build_object("
                                "       'job_key', job_key,"
                                "       'status', status,"
                                "       'result_summary', coalesce(result_summary, '')"
                                "     ) ORDER BY updated_at DESC), '[]'::jsonb)"
                                "     FROM public.memory_llm_job_queue"
                                "     WHERE status IN ('pending', 'running', 'leased')),"
                                "  'server.py:recall_context',"
                                "  true,"
                                "  now())",
                                [session_key],
                            )
                            logger.info('  [recall_context] Flushed open work to memory_context_notes')
                        except Exception as _flush_e:
                            logger.warning('  [recall_context] Flush before erase failed (non-fatal): %s', _flush_e)
                        logger.info('  [recall_context] Erasing slot for clean context')
                        await erase_llama_slot(_slot_endpoint, _slot_model, 0)
        except Exception as _e:
            logger.warning('  [recall_context] Slot check/erase failed: %s', _e)
        recall_n = _clamp_limit(recall_limit, 1, 50)
        history_n = _clamp_limit(max_history_blocks, 1, MAX_MEMORYDB_CHAT_BLOCKS)
        prompt_rows = await sql_driver.execute_query(
            """
            SELECT compiled_prompt, detected_intent, matched_rule_keys, matched_tool_keys, matched_categories
            FROM public.zorg_compile_system_prompt(%s, %s::jsonb)
            """,
            [user_request, _to_json_text(context or {})],
        )
        recall_rows = await sql_driver.execute_query(
            """
            SELECT source_type, source_id, path, line_start, line_end, priority, content,
                   recall_mode, rank, score, score_reason, metadata
            FROM public.memory_recall_v2(%s, %s, %s::jsonb)
            """,
            [user_request, recall_n, _to_json_text(context or {})],
        )
        history_rows = await sql_driver.execute_query(
            """
            SELECT id::text, session_key, role, content, metadata, created_at
            FROM public.lan_chat_messages
            WHERE session_key = %s
              AND coalesce(metadata->>'kind', '') NOT IN ('model_stream_delta', 'context_block')
            ORDER BY created_at DESC
            LIMIT %s
            """,
            [session_key, history_n],
        )
        history_budget = max(256, int(max(1, context_window_tokens) * 0.35))
        selected: list[dict[str, Any]] = []
        used_tokens = 0
        for row in _rows(history_rows):
            estimate = _token_estimate(row.get("content", ""))
            if selected and used_tokens + estimate > history_budget:
                continue
            row["token_estimate"] = estimate
            selected.append(row)
            used_tokens += estimate
        selected.reverse()
        compiled = _rows(prompt_rows)
        prompt = compiled[0].get("compiled_prompt", "") if compiled else ""
        result = {
            "system_prompt": (prompt + "\n\n" + MEMORYDB_SYSTEM_TOOL_INSTRUCTIONS).strip(),
            "compile_result": compiled[0] if compiled else {},
            "semantic_recall": _rows(recall_rows),
            "chat_history_blocks": selected,
            "history_block_count": len(selected),
            "history_block_cap": MAX_MEMORYDB_CHAT_BLOCKS,
            "history_token_estimate": used_tokens,
            "history_token_budget": history_budget,
            "session_key": session_key,
        }
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error building MemoryDB context package: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Return bounded chat-history blocks from the existing lan_chat_messages table, capped at 30 blocks.")
async def memorydb_get_chat_history_blocks(
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    max_blocks: int = Field(description="Maximum returned blocks; hard capped at 30", default=30),
    context_window_tokens: int = Field(description="Available model context window estimate", default=8192),
    include_stream_blocks: bool = Field(description="Include raw stream delta rows", default=False),
) -> ResponseType:
    """Return bounded chat history blocks from the existing table."""
    try:
        sql_driver = await get_sql_driver()
        block_limit = _clamp_limit(max_blocks, 1, MAX_MEMORYDB_CHAT_BLOCKS)
        excluded_clause = "" if include_stream_blocks else "AND coalesce(metadata->>'kind', '') NOT IN ('model_stream_delta', 'context_block')"
        rows = await sql_driver.execute_query(
            f"""
            SELECT id::text, session_key, role, content, metadata, created_at
            FROM public.lan_chat_messages
            WHERE session_key = %s
            {excluded_clause}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            [session_key, block_limit],
        )
        history_budget = max(256, int(max(1, context_window_tokens) * 0.35))
        selected: list[dict[str, Any]] = []
        used_tokens = 0
        for row in _rows(rows):
            estimate = _token_estimate(row.get("content", ""))
            if selected and used_tokens + estimate > history_budget:
                continue
            row["token_estimate"] = estimate
            selected.append(row)
            used_tokens += estimate
        selected.reverse()
        return format_text_response(_to_json_text({
            "session_key": session_key,
            "blocks": selected,
            "block_count": len(selected),
            "block_cap": MAX_MEMORYDB_CHAT_BLOCKS,
            "token_estimate": used_tokens,
            "token_budget": history_budget,
        }))
    except Exception as e:
        logger.error(f"Error getting MemoryDB chat-history blocks: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Search older chat history in the existing lan_chat_messages table without loading all history.")
async def memorydb_search_chat_history(
    query: str = Field(description="Search text"),
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    limit: int = Field(description="Maximum matched blocks; hard capped at 20", default=10),
) -> ResponseType:
    """Search existing chat history with a bounded result set."""
    try:
        sql_driver = await get_sql_driver()
        search_limit = _clamp_limit(limit, 1, MAX_MEMORYDB_CHAT_BLOCKS)
        rows = await sql_driver.execute_query(
            """
            SELECT id::text, session_key, role, content, metadata, created_at
            FROM public.lan_chat_messages
            WHERE session_key = %s
              AND content ILIKE %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            [session_key, f"%{query}%", search_limit],
        )
        results = _rows(rows)
        results.reverse()
        return format_text_response(_to_json_text({"query": query, "session_key": session_key, "results": results, "limit": search_limit}))
    except Exception as e:
        logger.error(f"Error searching MemoryDB chat history: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Store an exposed model output stream chunk in MemoryDB. Captures only fields the model/server exposes.")
async def memorydb_stream_model_output(
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    content_delta: str = Field(description="Visible assistant content delta", default=""),
    reasoning_content_delta: str = Field(description="Exposed reasoning_content delta, if the model/server provides it", default=""),
    metadata: dict[str, Any] = Field(description="Optional JSON metadata such as model id, chunk index, timing", default={}),
) -> ResponseType:
    """Persist one exposed model output stream chunk."""
    try:
        sql_driver = await get_sql_driver()
        role = "assistant"
        content = content_delta if content_delta else reasoning_content_delta
        rows = await sql_driver.execute_query(
            """
            INSERT INTO public.lan_chat_messages(session_key, role, content, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id::text, session_key, role, created_at, metadata
            """,
            [session_key, role, content, _metadata(metadata, kind="model_stream_delta", source="postgres_mcp", has_reasoning=bool(reasoning_content_delta))],
        )
        return format_text_response(_to_json_text({"stored": _rows(rows)[0] if rows else None}))
    except Exception as e:
        logger.error(f"Error streaming model output into MemoryDB: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Commit the final assistant response and exposed metadata into MemoryDB.")
async def memorydb_commit_turn(
    assistant_content: str = Field(description="Final visible assistant response"),
    session_key: str = Field(description="Conversation/session key", default=DEFAULT_MEMORYDB_SESSION_KEY),
    reasoning_content: str = Field(description="Exposed reasoning_content, if provided by the model/server", default=""),
    metadata: dict[str, Any] = Field(description="Optional JSON metadata: usage, timings, finish reason, model id", default={}),
) -> ResponseType:
    """Persist final assistant output for a MemoryDB-backed turn."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            INSERT INTO public.lan_chat_messages(session_key, role, content, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id::text, session_key, role, created_at, metadata
            """,
            [session_key, "assistant", assistant_content, _metadata(metadata, kind="assistant_final", source="postgres_mcp", reasoning_content=reasoning_content)],
        )
        return format_text_response(_to_json_text({"stored": _rows(rows)[0] if rows else None}))
    except Exception as e:
        logger.error(f"Error committing MemoryDB turn: {e}")
        return format_error_response(str(e))



@mcp.tool(description='Scan an LLM endpoint to detect serving system, context window, and API format. Results are stored in llm_registry.')
async def memorydb_detect_llm_context(
    endpoint: str = Field(description='LLM endpoint URL (e.g. http://localhost:9001)'),
    auth_token: str = Field(description='Optional Bearer token for authenticated endpoints', default=''),
    force_rescan: bool = Field(description='Force re-scan even if endpoint is in registry', default=False),
) -> ResponseType:
    'Detect LLM serving system and context window by probing the endpoint.'
    try:
        endpoint = endpoint.rstrip('/')
        if not force_rescan:
            existing = await get_llm_context_from_registry(endpoint)
            if existing:
                return format_text_response(_to_json_text({'source': 'registry_cache', **existing}))
        result = await scan_llm_endpoint(endpoint, auth_token or None)
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f'Error scanning LLM endpoint: {e}')
        return format_error_response(str(e))




# ============================================================
# llama.cpp Slot Management
# ============================================================

def _llama_model_candidates(model: str):
    if not model:
        return []
    cands = [model]
    for suf in ("--MTP", "-MTP"):
        if not model.endswith(suf):
            cands.append(model + suf)
    return cands


async def get_llama_slot_state(endpoint: str, model: str) -> dict:
    """Get current slot state from llama.cpp server."""
    endpoint = endpoint.rstrip("/")
    last_err = "no response from slots endpoint"
    for cand in _llama_model_candidates(model):
        url = f"{endpoint}/slots?model={cand}"
        resp = await asyncio.to_thread(_http_get_json, url)
        if resp is None:
            last_err = "no response from slots endpoint"
            continue
        slots = resp if isinstance(resp, list) else [resp]
        if not slots:
            last_err = "no slots reported for model"
            continue
        result = []
        for s in slots:
            result.append({
                "id": s.get("id"),
                "n_ctx": s.get("n_ctx"),
                "n_prompt_tokens": s.get("n_prompt_tokens", 0),
                "n_prompt_tokens_processed": s.get("n_prompt_tokens_processed", 0),
                "n_prompt_tokens_cache": s.get("n_prompt_tokens_cache", 0),
                "is_processing": s.get("is_processing", False),
            })
        return {"slots": result, "model": cand}
    return {"error": last_err}


async def erase_llama_slot(endpoint: str, model: str, slot_id: int = 0) -> dict:
    """Erase (clear) a llama.cpp slot's KV cache."""
    endpoint = endpoint.rstrip("/")
    last_err = "no slots found for model"
    for cand in _llama_model_candidates(model):
        url = f"{endpoint}/slots/{slot_id}?action=erase"
        req = urllib.request.Request(url, data=json.dumps({"model": cand}).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            resp = await asyncio.to_thread(
                lambda r=req: urllib.request.urlopen(r, timeout=30).read().decode()
            )
            return json.loads(resp)
        except Exception as e:
            last_err = str(e)
            continue
    return {"error": last_err}


async def save_llama_slot(endpoint: str, model: str, slot_id: int = 0, filename: str = "context.bin") -> dict:
    """Save a llama.cpp slot's KV cache to file."""
    endpoint = endpoint.rstrip("/")
    url = f"{endpoint}/slots/{slot_id}?action=save"
    body = json.dumps({"model": model, "filename": filename}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    resp = await asyncio.to_thread(
        lambda: urllib.request.urlopen(req, timeout=120).read().decode()
    )
    return json.loads(resp)


async def restore_llama_slot(endpoint: str, model: str, slot_id: int = 0, filename: str = "context.bin") -> dict:
    """Restore a llama.cpp slot's KV cache from file."""
    endpoint = endpoint.rstrip("/")
    url = f"{endpoint}/slots/{slot_id}?action=restore"
    body = json.dumps({"model": model, "filename": filename}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    resp = await asyncio.to_thread(
        lambda: urllib.request.urlopen(req, timeout=120).read().decode()
    )
    return json.loads(resp)


async def get_compaction_targets_from_db() -> list:
    """Read active compaction targets from context_compaction_config, ordered by priority."""
    sql_driver = await get_sql_driver()
    rows = await sql_driver.execute_query(
        "SELECT priority, endpoint, model_name, serving_system, context_window, num_ctx "
        "FROM public.context_compaction_config WHERE is_active = true ORDER BY priority"
    )
    return _rows(rows)


async def update_compaction_health(endpoint: str, healthy: bool, sql_driver=None) -> None:
    """Update health tracking for a compaction target."""
    if sql_driver is None:
        sql_driver = await get_sql_driver()
    if healthy:
        await sql_driver.execute_query(
            "UPDATE public.context_compaction_config SET last_healthy_at = now(), failure_count = 0, updated_at = now() WHERE endpoint = %s",
            [endpoint],
        )
    else:
        await sql_driver.execute_query(
            "UPDATE public.context_compaction_config SET failure_count = failure_count + 1, updated_at = now() WHERE endpoint = %s",
            [endpoint],
        )


async def prepare_context_for_llm(
    endpoint: str, model: str, session_key: str,
    context_window_tokens: int, token_budget: int,
) -> tuple:
    """
    Brain-context preparation:
    1. Check slot state, erase if utilization > 70%
    2. Build fresh context (no stale data)
    3. Track context items
    Returns (should_erase, slot_info)
    """
    endpoint = endpoint.rstrip("/")
    # Check slot state
    slot_info = await get_llama_slot_state(endpoint, model)
    slots = slot_info.get("slots", [])
    should_erase = False
    if slots:
        s = slots[0]
        n_ctx = s.get("n_ctx", 131072)
        # Real-time usage: use slot API token count when exposed; else derive
        # from is_processing so the tracker reflects live state, not 0.
        used = s.get("n_prompt_tokens", None)
        if used is None:
            used = n_ctx if s.get("is_processing", False) else 0
        utilization = used / n_ctx if n_ctx > 0 else 0
        if utilization > 0.70 or used == 0:
            should_erase = True
    
    # Update slot state in DB
    sql_driver = await get_sql_driver()
    slots_data = slots[0] if slots else {"id": 0, "n_ctx": context_window_tokens, "n_prompt_tokens": 0}
    await sql_driver.execute_query(
        """INSERT INTO public.context_slot_state 
           (session_key, llm_endpoint, model_name, n_ctx, n_prompt_tokens, n_prompt_tokens_processed, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s, now())
           ON CONFLICT (session_key, llm_endpoint) DO UPDATE SET
           n_ctx = EXCLUDED.n_ctx, n_prompt_tokens = EXCLUDED.n_prompt_tokens,
           n_prompt_tokens_processed = EXCLUDED.n_prompt_tokens_processed, updated_at = now()""",
        [session_key, endpoint, model, 
         slots_data.get("n_ctx", context_window_tokens),
         slots_data.get("n_prompt_tokens", 0),
         slots_data.get("n_prompt_tokens_processed", 0)],
    )
    
    return should_erase, slot_info




@mcp.tool(description='Get or manage compaction target configuration. Use action to list, add, remove, or update targets.')
async def memorydb_compaction_config(
    action: str = Field(description='Action: list, add, remove, update, health', default='list'),
    endpoint: str = Field(description='Endpoint URL (for add/remove/update)', default=''),
    model_name: str = Field(description='Model name (for add/update)', default=''),
    priority: int = Field(description='Priority order (for add)', default=0),
    serving_system: str = Field(description='serving system: ollama, llama_cpp, openai_compatible (for add)', default='ollama'),
    context_window: int = Field(description='Context window size (for add)', default=131072),
    num_ctx: int = Field(description='Per-request num_ctx for ollama (for add)', default=0),
    is_active: bool = Field(description='Active flag (for update)', default=True),
) -> ResponseType:
    """Manage the self-configurable compaction targets."""
    try:
        sql_driver = await get_sql_driver()
        if action == "list":
            rows = await sql_driver.execute_query(
                "SELECT priority, endpoint, model_name, serving_system, context_window, num_ctx, "
                "is_active, last_healthy_at, failure_count FROM public.context_compaction_config ORDER BY priority"
            )
            return format_text_response(_to_json_text({"targets": _rows(rows)}))
        elif action == "add":
            if not endpoint:
                return format_error_response("endpoint required for add")
            await sql_driver.execute_query(
                """INSERT INTO public.context_compaction_config 
                   (priority, endpoint, model_name, serving_system, context_window, num_ctx, is_active)
                   VALUES (%s, %s, %s, %s, %s, %s, true)
                   ON CONFLICT (endpoint) DO UPDATE SET
                   model_name = EXCLUDED.model_name, serving_system = EXCLUDED.serving_system,
                   context_window = EXCLUDED.context_window, num_ctx = EXCLUDED.num_ctx,
                   priority = EXCLUDED.priority, updated_at = now()""",
                [priority, endpoint, model_name or None, serving_system, context_window, num_ctx or None],
            )
            return format_text_response(_to_json_text({"added": endpoint, "priority": priority}))
        elif action == "remove":
            if not endpoint:
                return format_error_response("endpoint required for remove")
            await sql_driver.execute_query(
                "DELETE FROM public.context_compaction_config WHERE endpoint = %s", [endpoint]
            )
            return format_text_response(_to_json_text({"removed": endpoint}))
        elif action == "update":
            if not endpoint:
                return format_error_response("endpoint required for update")
            updates = []
            params = []
            if model_name:
                updates.append("model_name = %s"); params.append(model_name)
            if serving_system:
                updates.append("serving_system = %s"); params.append(serving_system)
            if context_window:
                updates.append("context_window = %s"); params.append(context_window)
            if num_ctx:
                updates.append("num_ctx = %s"); params.append(num_ctx)
            updates.append("is_active = %s"); params.append(is_active)
            updates.append("updated_at = now()")
            params.append(endpoint)
            await sql_driver.execute_query(
                f"UPDATE public.context_compaction_config SET {', '.join(updates)} WHERE endpoint = %s",
                params,
            )
            return format_text_response(_to_json_text({"updated": endpoint}))
        elif action == "health":
            # Probe all active targets
            targets = await get_compaction_targets_from_db()
            results = []
            for t in targets:
                ep = t["endpoint"]
                healthy = False
                try:
                    if ep.endswith("/api/chat"):
                        models_url = ep.rsplit("/", 2)[0] + "/api/tags"
                        resp = await asyncio.to_thread(_http_get_json, models_url)
                        healthy = resp is not None and "models" in resp
                    else:
                        models_url = ep.rstrip("/") + "/v1/models"
                        resp = await asyncio.to_thread(_http_get_json, models_url)
                        healthy = resp is not None and "data" in resp
                except Exception:
                    healthy = False
                await update_compaction_health(ep, healthy, sql_driver)
                results.append({"endpoint": ep, "healthy": healthy, "model": t.get("model_name")})
            return format_text_response(_to_json_text({"targets": results}))
        else:
            return format_error_response(f"Unknown action: {action}")
    except Exception as e:
        logger.error(f"Error in memorydb_compaction_config: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Get current llama.cpp slot state (utilization, free tokens).')
async def memorydb_slot_state(
    endpoint: str = Field(description='LLM endpoint URL', default='http://localhost:9001'),
    model: str = Field(description='Model name', default=''),
) -> ResponseType:
    """Get slot utilization for an llama.cpp endpoint."""
    try:
        if not model:
            # Auto-detect from registry
            registry = await get_llm_context_from_registry(endpoint.rstrip("/"))
            model = registry.get("model_name") if registry else ""
            if not model:
                return format_error_response("model required (not found in registry)")
        result = await get_llama_slot_state(endpoint, model)
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error in memorydb_slot_state: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Erase (clear) a llama.cpp slot to free all context tokens.')
async def memorydb_slot_erase(
    endpoint: str = Field(description='LLM endpoint URL', default='http://localhost:9001'),
    model: str = Field(description='Model name', default=''),
    slot_id: int = Field(description='Slot ID', default=0),
) -> ResponseType:
    """Clear the KV cache of an llama.cpp slot."""
    try:
        if not model:
            registry = await get_llm_context_from_registry(endpoint.rstrip("/"))
            model = registry.get("model_name") if registry else ""
            if not model:
                return format_error_response("model required")
        result = await erase_llama_slot(endpoint, model, slot_id)
        # Update DB
        sql_driver = await get_sql_driver()
        await sql_driver.execute_query(
            "UPDATE public.context_slot_state SET n_prompt_tokens = 0, last_erased_at = now(), updated_at = now() "
            "WHERE llm_endpoint = %s", [endpoint.rstrip("/")]
        )
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error in memorydb_slot_erase: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Save a llama.cpp slot KV cache to file for later restore.')
async def memorydb_slot_save(
    endpoint: str = Field(description='LLM endpoint URL', default='http://localhost:9001'),
    model: str = Field(description='Model name', default=''),
    slot_id: int = Field(description='Slot ID', default=0),
    filename: str = Field(description='Save filename', default='context.bin'),
) -> ResponseType:
    """Persist slot state to file."""
    try:
        if not model:
            registry = await get_llm_context_from_registry(endpoint.rstrip("/"))
            model = registry.get("model_name") if registry else ""
            if not model:
                return format_error_response("model required")
        result = await save_llama_slot(endpoint, model, slot_id, filename)
        sql_driver = await get_sql_driver()
        await sql_driver.execute_query(
            "UPDATE public.context_slot_state SET last_saved_at = now(), save_filename = %s, updated_at = now() "
            "WHERE llm_endpoint = %s", [filename, endpoint.rstrip("/")]
        )
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error in memorydb_slot_save: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Restore a llama.cpp slot KV cache from a saved file.')
async def memorydb_slot_restore(
    endpoint: str = Field(description='LLM endpoint URL', default='http://localhost:9001'),
    model: str = Field(description='Model name', default=''),
    slot_id: int = Field(description='Slot ID', default=0),
    filename: str = Field(description='Filename to restore', default='context.bin'),
) -> ResponseType:
    """Restore slot state from file."""
    try:
        if not model:
            registry = await get_llm_context_from_registry(endpoint.rstrip("/"))
            model = registry.get("model_name") if registry else ""
            if not model:
                return format_error_response("model required")
        result = await restore_llama_slot(endpoint, model, slot_id, filename)
        return format_text_response(_to_json_text(result))
    except Exception as e:
        logger.error(f"Error in memorydb_slot_restore: {e}")
        return format_error_response(str(e))


@mcp.tool(description='View what context items are tracked for a session.')
async def memorydb_context_items(
    session_key: str = Field(description='Session key', default='agent:main:lan-chat'),
    endpoint: str = Field(description='LLM endpoint', default='http://localhost:9001'),
) -> ResponseType:
    """Show tracked context items for a session."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            "SELECT context_items, n_ctx, n_prompt_tokens, last_erased_at, last_saved_at, save_filename "
            "FROM public.context_slot_state WHERE session_key = %s AND llm_endpoint = %s",
            [session_key, endpoint.rstrip("/")]
        )
        data = _rows(rows)[0] if rows else None
        return format_text_response(_to_json_text(data or {"info": "no slot state tracked"}))
    except Exception as e:
        logger.error(f"Error in memorydb_context_items: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Add a context item to the tracked context for a session.')
async def memorydb_context_add_item(
    session_key: str = Field(description='Session key', default='agent:main:lan-chat'),
    endpoint: str = Field(description='LLM endpoint', default='http://localhost:9001'),
    item_type: str = Field(description='Item type: system_prompt, recall, history, rule, task', default='recall'),
    tokens: int = Field(description='Estimated token count', default=0),
    keys: str = Field(description='Comma-separated keys or identifiers', default=''),
    content: str = Field(description='Optional content summary', default=''),
) -> ResponseType:
    """Add a tracked context item."""
    try:
        sql_driver = await get_sql_driver()
        item = {
            "type": item_type,
            "tokens": tokens,
            "keys": keys.split(",") if keys else [],
            "content_preview": content[:200] if content else "",
            "added_at": "now",
        }
        await sql_driver.execute_query(
            """INSERT INTO public.context_slot_state (session_key, llm_endpoint, context_items, updated_at)
               VALUES (%s, %s, %s::jsonb, now())
               ON CONFLICT (session_key, llm_endpoint) DO UPDATE SET
               context_items = public.context_slot_state.context_items || EXCLUDED.context_items,
               updated_at = now()""",
            [session_key, endpoint.rstrip("/"), json.dumps([item])],
        )
        return format_text_response(_to_json_text({"added": item}))
    except Exception as e:
        logger.error(f"Error in memorydb_context_add_item: {e}")
        return format_error_response(str(e))


@mcp.tool(description='Remove context items of a given type from the tracked context.')
async def memorydb_context_remove_item(
    session_key: str = Field(description='Session key', default='agent:main:lan-chat'),
    endpoint: str = Field(description='LLM endpoint', default='http://localhost:9001'),
    item_type: str = Field(description='Type to remove: recall, history, task, rule, or "all"', default='all'),
) -> ResponseType:
    """Remove tracked context items by type."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            "SELECT context_items FROM public.context_slot_state WHERE session_key = %s AND llm_endpoint = %s",
            [session_key, endpoint.rstrip("/")]
        )
        current = _rows(rows)[0].get("context_items", []) if rows else []
        if item_type == "all":
            new_items = []
        else:
            new_items = [i for i in current if i.get("type") != item_type]
        removed = [i for i in current if i.get("type") == item_type] if item_type != "all" else current
        await sql_driver.execute_query(
            "UPDATE public.context_slot_state SET context_items = %s::jsonb, updated_at = now() "
            "WHERE session_key = %s AND llm_endpoint = %s",
            [json.dumps(new_items), session_key, endpoint.rstrip("/")]
        )
        return format_text_response(_to_json_text({"removed": removed, "remaining_count": len(new_items)}))
    except Exception as e:
        logger.error(f"Error in memorydb_context_remove_item: {e}")
        return format_error_response(str(e))



async def memorydb_http_health(request: Request) -> JSONResponse:
    """Cheap JSON health endpoint for browser-side MemoryDB history clients."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            "SELECT current_database() AS database, current_user AS user, now() AS server_time"
        )
        return JSONResponse(_jsonable({"ok": True, "identity": _rows(rows)[0] if rows else None}))
    except Exception as e:
        logger.error(f"Error checking MemoryDB HTTP health: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


async def memorydb_http_store_message(request: Request) -> JSONResponse:
    """Upsert one llama.cpp Web UI chat message into lan_chat_messages."""
    try:
        payload = await request.json()
        session_key = str(payload.get("session_key") or DEFAULT_MEMORYDB_SESSION_KEY)
        role = str(payload.get("role") or "")
        content = str(payload.get("content") or "")
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        message_id = str(metadata.get("message_id") or payload.get("message_id") or "")

        if role not in {"system", "user", "assistant", "tool"}:
            return JSONResponse({"ok": False, "error": f"Unsupported role: {role}"}, status_code=400)
        if not content and metadata.get("message_type") == "root":
            return JSONResponse({"ok": True, "skipped": "root"})

        sql_driver = await get_sql_driver()
        merged_metadata = _metadata(
            metadata,
            kind="llama_ui_message",
            source="postgres_mcp_http",
            message_id=message_id or None,
        )

        if message_id:
            rows = await sql_driver.execute_query(
                """
                WITH updated AS (
                    UPDATE public.lan_chat_messages
                    SET role = %s,
                        content = %s,
                        metadata = metadata || %s::jsonb,
                        created_at = COALESCE(to_timestamp((%s)::double precision / 1000.0), created_at)
                    WHERE session_key = %s
                      AND metadata->>'message_id' = %s
                    RETURNING id::text, session_key, role, created_at, metadata
                ),
                inserted AS (
                    INSERT INTO public.lan_chat_messages(session_key, role, content, metadata, created_at)
                    SELECT %s, %s, %s, %s::jsonb, COALESCE(to_timestamp((%s)::double precision / 1000.0), now())
                    WHERE NOT EXISTS (SELECT 1 FROM updated)
                    RETURNING id::text, session_key, role, created_at, metadata
                )
                SELECT * FROM updated
                UNION ALL
                SELECT * FROM inserted
                """,
                [
                    role,
                    content,
                    merged_metadata,
                    metadata.get("timestamp"),
                    session_key,
                    message_id,
                    session_key,
                    role,
                    content,
                    merged_metadata,
                    metadata.get("timestamp"),
                ],
            )
        else:
            rows = await sql_driver.execute_query(
                """
                INSERT INTO public.lan_chat_messages(session_key, role, content, metadata)
                VALUES (%s, %s, %s, %s::jsonb)
                RETURNING id::text, session_key, role, created_at, metadata
                """,
                [session_key, role, content, merged_metadata],
            )

        return JSONResponse(_jsonable({"ok": True, "stored": _rows(rows)[0] if rows else None}))
    except Exception as e:
        logger.error(f"Error storing MemoryDB HTTP chat message: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


async def memorydb_http_history(request: Request) -> JSONResponse:
    """Return recent chat history bounded by assistant response count and token budget."""
    try:
        payload = await request.json()
        session_key = str(payload.get("session_key") or DEFAULT_MEMORYDB_SESSION_KEY)
        max_responses = _clamp_limit(int(payload.get("max_responses") or 30), 1, MAX_MEMORYDB_CHAT_BLOCKS)
        context_window_tokens = int(payload.get("context_window_tokens") or 8192)
        scan_limit = min(200, max(30, max_responses * 4 + 20))

        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT id::text, session_key, role, content, metadata, created_at
            FROM public.lan_chat_messages
            WHERE session_key = %s
              AND coalesce(metadata->>'kind', '') NOT IN ('model_stream_delta', 'context_block')
              AND coalesce(metadata->>'message_type', '') <> 'root'
            ORDER BY created_at DESC
            LIMIT %s
            """,
            [session_key, scan_limit],
        )

        history_budget = max(256, int(max(1, context_window_tokens) * 0.35))
        selected: list[dict[str, Any]] = []
        used_tokens = 0
        assistant_count = 0

        for row in _rows(rows):
            if row.get("role") == "assistant":
                if assistant_count >= max_responses:
                    break
                assistant_count += 1

            estimate = _token_estimate(row.get("content", ""))
            if selected and used_tokens + estimate > history_budget:
                continue
            row["token_estimate"] = estimate
            selected.append(row)
            used_tokens += estimate

        selected.reverse()
        return JSONResponse(
            _jsonable({
                "ok": True,
                "session_key": session_key,
                "blocks": selected,
                "block_count": len(selected),
                "assistant_response_cap": max_responses,
                "block_cap": MAX_MEMORYDB_CHAT_BLOCKS,
                "token_estimate": used_tokens,
                "token_budget": history_budget,
            })
        )
    except Exception as e:
        logger.error(f"Error returning MemoryDB HTTP chat history: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
                elif part.get("type"):
                    parts.append(f"[{part.get('type')}]")
        return "\n".join(parts)
    if content is None:
        return ""
    return _to_json_text(content)


def _message_token_estimate(message: dict[str, Any]) -> int:
    return _token_estimate(f"{message.get('role', '')}\n{_message_text(message)}")


def _truncate_context_message(message: dict[str, Any], token_budget: int) -> dict[str, Any] | None:
    """Return a copy of one message shortened enough to fit the remaining budget."""
    if token_budget <= 1:
        return None

    truncated = dict(message)
    role_cost = _token_estimate(str(truncated.get("role", "")))
    available_text_tokens = max(1, token_budget - role_cost - 8)
    max_chars = max(128, available_text_tokens * 4)
    text = _message_text(truncated)

    if not text:
        return truncated if _message_token_estimate(truncated) <= token_budget else None

    if len(text) > max_chars:
        text = text[-max_chars:]
        text = "[older content trimmed by MemoryDB context budget]\n" + text

    truncated["content"] = text

    while _message_token_estimate(truncated) > token_budget and len(text) > 256:
        text = text[len(text) // 4 :]
        truncated["content"] = "[older content trimmed by MemoryDB context budget]\n" + text

    return truncated if _message_token_estimate(truncated) <= token_budget else None


def _normalize_context_message(message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    role = str(message.get("role") or "")
    if role not in {"system", "user", "assistant", "tool"}:
        return None
    normalized: dict[str, Any] = {"role": role, "content": message.get("content") or ""}
    if message.get("name"):
        normalized["name"] = message.get("name")
    if message.get("tool_call_id"):
        normalized["tool_call_id"] = message.get("tool_call_id")
    if message.get("tool_calls"):
        normalized["tool_calls"] = message.get("tool_calls")
    return normalized


def _history_row_to_message(row: dict[str, Any]) -> dict[str, Any] | None:
    metadata = row.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    ui_message = metadata.get("ui_message") if isinstance(metadata, dict) else None
    if isinstance(ui_message, dict):
        normalized = _normalize_context_message(ui_message)
        if normalized:
            return normalized
    return _normalize_context_message({"role": row.get("role"), "content": row.get("content")})


def _context_signature(message: dict[str, Any]) -> str:
    return f"{message.get('role')}:{_message_text(message)[:500]}"


def _context_token_total(messages: list[dict[str, Any]]) -> int:
    return sum(_message_token_estimate(message) for message in messages)


def _pinned_system_context(
    system_messages: list[dict[str, Any]],
    memory_context: str,
) -> list[dict[str, Any]]:
    """Temporarily emit no system messages from prepared-context."""
    return []

def _trim_context_to_budget(
    pinned_messages: list[dict[str, Any]],
    recent_messages: list[dict[str, Any]],
    token_budget: int,
) -> list[dict[str, Any]]:
    selected = [*pinned_messages]
    used = _context_token_total(selected)
    kept_recent: list[dict[str, Any]] = []
    for message in reversed(recent_messages):
        estimate = _message_token_estimate(message)
        if used + estimate <= token_budget:
            kept_recent.append(message)
            used += estimate
            continue

        if not kept_recent and used < token_budget:
            truncated = _truncate_context_message(message, token_budget - used)
            if truncated:
                kept_recent.append(truncated)
                used += _message_token_estimate(truncated)
    kept_recent.reverse()
    return [*selected, *kept_recent]


async def _latest_context_compaction(sql_driver: Union[SqlDriver, SafeSqlDriver], session_key: str) -> str:
    rows = await sql_driver.execute_query(
        """
        SELECT content
        FROM public.lan_chat_messages
        WHERE session_key = %s
          AND metadata->>'kind' = 'prepared_context_compaction'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [session_key],
    )
    data = _rows(rows)
    return str(data[0].get("content") or "") if data else ""


async def _recent_context_messages(
    sql_driver: Union[SqlDriver, SafeSqlDriver],
    session_key: str,
    max_responses: int,
) -> list[dict[str, Any]]:
    scan_limit = min(240, max(40, max_responses * 4 + 30))
    rows = await sql_driver.execute_query(
        """
        SELECT id::text, session_key, role, content, metadata, created_at
        FROM public.lan_chat_messages
        WHERE session_key = %s
          AND coalesce(metadata->>'kind', '') NOT IN (
              'model_stream_delta',
              'context_block',
              'prepared_context_compaction'
          )
          AND coalesce(metadata->>'message_type', '') <> 'root'
        ORDER BY created_at DESC
        LIMIT %s
        """,
        [session_key, scan_limit],
    )

    selected: list[dict[str, Any]] = []
    assistant_count = 0
    for row in _rows(rows):
        if row.get("role") == "assistant":
            if assistant_count >= max_responses:
                break
            assistant_count += 1
        message = _history_row_to_message(row)
        if message:
            selected.append(message)
    selected.reverse()
    return selected


def _compact_context_with_llm(
    *,
    endpoint: str,
    model: str,
    system_prompt_present: bool,
    previous_summary: str,
    messages: list[dict[str, Any]],
    max_tokens: int,
    timeout_seconds: int,
) -> str:
    compact_input = {
        "previous_compacted_working_memory": previous_summary,
        "messages_to_compact": [
            {"role": message.get("role"), "content": _message_text(message)}
            for message in messages
            if message.get("role") != "system"
        ],
    }
    messages_payload = [
        {
            "role": "system",
            "content": (
                "You compact working context for a local LLM chat system. "
                "Keep only facts, constraints, decisions, open tasks, current state, "
                "important user preferences, and unresolved errors needed for future work. "
                "Remove stale, repeated, low-value, and already-resolved detail. "
                "Do not include or rewrite the pinned system prompt; it is kept separately. "
                f"System prompt present separately: {system_prompt_present}. "
                "Return concise plain text only."
            ),
        },
        {"role": "user", "content": _to_json_text(compact_input)},
    ]
    if endpoint.endswith("/api/chat"):
        body = {
            "model": model,
            "messages": messages_payload,
            "stream": False,
            "options": {"temperature": 0, "num_predict": max_tokens, "num_ctx": num_ctx},
        }
    else:
        body = {
            "model": model,
            "messages": messages_payload,
            "temperature": 0,
            "max_tokens": max_tokens,
            "stream": False,
        }
    request = urllib.request.Request(
        endpoint,
        data=_to_json_text(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if endpoint.endswith("/api/chat"):
        message = payload.get("message") or {}
        compacted = str(message.get("content") or message.get("reasoning") or message.get("thinking") or "").strip()
    else:
        message = payload["choices"][0]["message"]
        compacted = str(message.get("content") or message.get("reasoning") or message.get("thinking") or "").strip()
    if not compacted:
        raise ValueError("empty compaction response")
    return compacted


def _resolve_compaction_model(endpoint: str, preferred_model: str | None) -> str:
    if preferred_model:
        return preferred_model
    models_url = endpoint.rsplit("/", 2)[0] + "/models"
    with urllib.request.urlopen(models_url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    models = payload.get("data") or []
    if not models:
        raise ValueError("No models available for context compaction")
    return str(models[0]["id"])


async def _store_context_compaction(
    sql_driver: Union[SqlDriver, SafeSqlDriver],
    session_key: str,
    content: str,
    metadata: dict[str, Any],
) -> None:
    await sql_driver.execute_query(
        """
        INSERT INTO public.lan_chat_messages(session_key, role, content, metadata)
        VALUES (%s, 'system', %s, %s::jsonb)
        """,
        [session_key, content, _metadata(metadata, kind="prepared_context_compaction")],
    )


async def memorydb_http_prepare_context(request: Request) -> JSONResponse:
    """Prepare a model context package while preserving existing chat-history behavior."""
    try:
        payload = await request.json()
        session_key = str(payload.get("session_key") or DEFAULT_MEMORYDB_SESSION_KEY)
        max_responses = _clamp_limit(int(payload.get("max_responses") or 30), 1, MAX_MEMORYDB_CHAT_BLOCKS)
        
        # Dynamic context: caller value > registry lookup > default
        context_window_tokens = int(payload.get("context_window_tokens") or 0)
        llm_endpoint = str(payload.get("llm_endpoint") or "")
        llm_context_info: dict | None = None
        if context_window_tokens > 0:
            pass
        elif llm_endpoint:
            llm_context_info = await get_llm_context_from_registry(llm_endpoint.rstrip("/"))
            context_window_tokens = int(llm_context_info.get("context_window") or DEFAULT_CONTEXT_WINDOW_TOKENS) if llm_context_info else DEFAULT_CONTEXT_WINDOW_TOKENS
        else:
            llm_context_info = await get_default_llm_context()
            context_window_tokens = int(llm_context_info.get("context_window") or DEFAULT_CONTEXT_WINDOW_TOKENS)
        token_budget = max(512, int(max(1, context_window_tokens) * 0.35))
        force_compaction = bool(payload.get("force_compaction"))

        # Brain-context: check llama.cpp slot state and erase if needed
        if llm_context_info and llm_context_info.get("serving_system") == "llama_cpp":
            try:
                slot_endpoint = (llm_context_info.get("endpoint") or "").rstrip("/")
                slot_model = llm_context_info.get("model_name") or ""
                if slot_endpoint and slot_model:
                    should_erase, slot_info = await prepare_context_for_llm(
                        slot_endpoint, slot_model, session_key,
                        context_window_tokens, token_budget,
                    )
                    if should_erase:
                        logger.info("  Erasing slot for clean context (utilization > 70%% or empty)")
                        erase_result = await erase_llama_slot(slot_endpoint, slot_model, 0)
                        logger.info("  Slot erased: %s", erase_result)
                        await sql_driver.execute_query(
                            "UPDATE public.context_slot_state SET n_prompt_tokens = 0, last_erased_at = now(), context_items = '[]'::jsonb, updated_at = now() WHERE session_key = %s AND llm_endpoint = %s",
                            [session_key, slot_endpoint],
                        )
            except Exception as e:
                logger.warning("  Slot check/erase failed: %s", e)

        candidate_messages = [
            message
            for message in (_normalize_context_message(item) for item in payload.get("messages") or [])
            if message
        ]
        system_messages = [message for message in candidate_messages if message.get("role") == "system"]
        candidate_non_system = [message for message in candidate_messages if message.get("role") != "system"]

        sql_driver = await get_sql_driver()
        previous_summary = await _latest_context_compaction(sql_driver, session_key)
        recent_messages = await _recent_context_messages(sql_driver, session_key, max_responses)

        seen = {_context_signature(message) for message in recent_messages}
        for message in candidate_non_system:
            signature = _context_signature(message)
            if signature not in seen:
                recent_messages.append(message)
                seen.add(signature)

        memory_context = previous_summary

        prepared = [*_pinned_system_context(system_messages, memory_context), *recent_messages]
        initial_estimate = _context_token_total(prepared)
        # Write real sent-context token count into tracker (overrides proxy from prepare_context_for_llm)
        if slot_endpoint:
            await sql_driver.execute_query(
                """UPDATE public.context_slot_state
                   SET n_prompt_tokens = %s, updated_at = now()
                   WHERE session_key = %s AND llm_endpoint = %s""",
                [initial_estimate, session_key, slot_endpoint],
            )
        compacted = False
        compaction_error: str | None = None

        if force_compaction:
            compact_source = [*_pinned_system_context([], memory_context), *recent_messages]
            # Keep context compaction off the large chat model. The browser may send its
            # selected model name, but postgres-mcp owns the compaction route so failures
            # do not crash the active llama.cpp generation process.
            # Use DB-configured compaction targets (self-configurable)
            db_targets = await get_compaction_targets_from_db()
            if not db_targets:
                db_targets = [
                    {"endpoint": DEFAULT_CONTEXT_COMPACTION_URL, "model_name": DEFAULT_CONTEXT_COMPACTION_MODEL, "serving_system": "ollama", "context_window": 131072, "num_ctx": 131072},
                ]
            compacted_text = ""
            last_compaction_error: str | None = None
            for target in db_targets:
                ep = target["endpoint"]
                configured_model = target.get("model_name") or None
                target_num_ctx = target.get("num_ctx") or 0
                try:
                    compacted_text = await asyncio.to_thread(
                        _compact_context_with_llm,
                        endpoint=ep,
                        model=_resolve_compaction_model(ep, configured_model),
                        system_prompt_present=bool(system_messages),
                        previous_summary=previous_summary,
                        messages=compact_source,
                        max_tokens=int(payload.get("compaction_max_tokens") or 2048),
                        timeout_seconds=int(payload.get("compaction_timeout_seconds") or 120),
                        num_ctx=target_num_ctx,
                    )
                    compaction_error = None
                    await update_compaction_health(ep, True)
                    break
                except (urllib.error.URLError, TimeoutError, KeyError, ValueError, json.JSONDecodeError, OSError) as e:
                    last_compaction_error = f"{ep}: {e}"
                    compacted_text = ""
                    await update_compaction_health(ep, False)
            if not compacted_text:
                compacted_text = previous_summary
                compaction_error = last_compaction_error

            if compacted_text:
                await _store_context_compaction(
                    sql_driver,
                    session_key,
                    compacted_text,
                    {
                        "source": "postgres-mcp-prepared-context",
                        "context_window_tokens": context_window_tokens,
                        "token_budget": token_budget,
                        "initial_token_estimate": initial_estimate,
                        "compaction_error": compaction_error,
                    },
                )
                memory_context = compacted_text
                compacted = True

            prepared = _trim_context_to_budget(
                _pinned_system_context(system_messages, memory_context),
                recent_messages,
                token_budget,
            )

        if _context_token_total(prepared) > token_budget:
            prepared = _trim_context_to_budget(
                _pinned_system_context(system_messages, memory_context),
                recent_messages,
                token_budget,
            )

        final_estimate = _context_token_total(prepared)
        response_body = {
            "ok": True,
            "session_key": session_key,
            "messages": prepared,
            "message_count": len(prepared),
            "context_window_tokens": context_window_tokens,
            "token_budget": token_budget,
            "token_estimate": final_estimate,
            "initial_token_estimate": initial_estimate,
            "assistant_response_cap": max_responses,
            "compacted": compacted,
            "compaction_error": compaction_error,
            "source": "postgres-mcp-prepared-context",
        }
        if llm_context_info:
            response_body["llm_context"] = {
                "serving_system": llm_context_info.get("serving_system"),
                "endpoint": llm_context_info.get("endpoint") or llm_endpoint,
                "model_name": llm_context_info.get("model_name"),
                "context_window": llm_context_info.get("context_window"),
                "supports_per_request_ctx": llm_context_info.get("supports_per_request_ctx"),
                "api_format": llm_context_info.get("api_format"),
            }
        return JSONResponse(_jsonable(response_body))
    except Exception as e:
        logger.error(f"Error preparing MemoryDB context: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


MEMORYDB_HTTP_ROUTES = [
    Route("/memorydb/chat/health", memorydb_http_health, methods=["GET"]),
    Route("/memorydb/chat/message", memorydb_http_store_message, methods=["POST"]),
    Route("/memorydb/chat/history", memorydb_http_history, methods=["POST"]),
    Route("/memorydb/context/prepare", memorydb_http_prepare_context, methods=["POST"]),
]


# Query function declaration without the decorator - we'll add it dynamically based on access mode
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze frequently executed queries in the database and recommend optimal indexes")
@validate_call
async def analyze_workload_indexes(
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze frequently executed queries in the database and recommend optimal indexes."""
    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_workload(max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing workload: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze a list of (up to 10) SQL queries and recommend optimal indexes")
@validate_call
async def analyze_query_indexes(
    queries: list[str] = Field(description="List of Query strings to analyze"),
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze a list of SQL queries and recommend optimal indexes."""
    if len(queries) == 0:
        return format_error_response("Please provide a non-empty list of queries to analyze.")
    if len(queries) > MAX_NUM_INDEX_TUNING_QUERIES:
        return format_error_response(f"Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries to analyze.")

    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_queries(queries=queries, max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyzes database health. Here are the available health checks:\n"
    "- index - checks for invalid, duplicate, and bloated indexes\n"
    "- connection - checks the number of connection and their utilization\n"
    "- vacuum - checks vacuum health for transaction id wraparound\n"
    "- sequence - checks sequences at risk of exceeding their maximum value\n"
    "- replication - checks replication health including lag and slots\n"
    "- buffer - checks for buffer cache hit rates for indexes and tables\n"
    "- constraint - checks for invalid constraints\n"
    "- all - runs all checks\n"
    "You can optionally specify a single health check or a comma-separated list of health checks. The default is 'all' checks."
)
async def analyze_db_health(
    health_type: str = Field(
        description=f"Optional. Valid values are: {', '.join(sorted([t.value for t in HealthType]))}.",
        default="all",
    ),
) -> ResponseType:
    """Analyze database health for specified components.

    Args:
        health_type: Comma-separated list of health check types to perform.
                    Valid values: index, connection, vacuum, sequence, replication, buffer, constraint, all
    """
    health_tool = DatabaseHealthTool(await get_sql_driver())
    result = await health_tool.health(health_type=health_type)
    return format_text_response(result)


@mcp.tool(
    name="get_top_queries",
    description=f"Reports the slowest or most resource-intensive queries using data from the '{PG_STAT_STATEMENTS}' extension.",
)
async def get_top_queries(
    sort_by: str = Field(
        description="Ranking criteria: 'total_time' for total execution time or 'mean_time' for mean execution time per call, or 'resources' "
        "for resource-intensive queries",
        default="resources",
    ),
    limit: int = Field(description="Number of queries to return when ranking based on mean_time or total_time", default=10),
) -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        top_queries_tool = TopQueriesCalc(sql_driver=sql_driver)

        if sort_by == "resources":
            result = await top_queries_tool.get_top_resource_queries()
            return format_text_response(result)
        elif sort_by == "mean_time" or sort_by == "total_time":
            # Map the sort_by values to what get_top_queries_by_time expects
            result = await top_queries_tool.get_top_queries_by_time(limit=limit, sort_by="mean" if sort_by == "mean_time" else "total")
        else:
            return format_error_response("Invalid sort criteria. Please use 'resources' or 'mean_time' or 'total_time'.")
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting slow queries: {e}")
        return format_error_response(str(e))


def _exception_contains_text(exc: BaseException, text: str) -> bool:
    if text in str(exc):
        return True
    children = getattr(exc, "exceptions", None)
    if children:
        return any(_exception_contains_text(child, text) for child in children)
    return False


class StaleSseInitializationGuard:
    def __init__(self, app: Any):
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        response_started = False
        async def guarded_send(message: dict[str, Any]) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)
        try:
            await self.app(scope, receive, guarded_send)
        except BaseException as exc:
            stale_init = "Received request before initialization was complete"
            if _exception_contains_text(exc, stale_init):
                logger.warning("Suppressed stale MCP SSE initialization race")
                if scope.get("type") == "http" and not response_started:
                    await send({"type": "http.response.start", "status": 409, "headers": [(b"content-type", b"application/json")]})
                    await send({"type": "http.response.body", "body": b"{\"ok\":false,\"error\":\"stale_mcp_sse_session\"}"})
                return
            raise


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PostgreSQL MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=AccessMode.UNRESTRICTED.value,
        help="Set SQL access mode: unrestricted (unrestricted) or restricted (read-only with protections)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse"],
        default="stdio",
        help="Select MCP transport: stdio (default) or sse",
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default="localhost",
        help="Host to bind SSE server to (default: localhost)",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=8000,
        help="Port for SSE server (default: 8000)",
    )

    args = parser.parse_args()

    # Store the access mode in the global variable
    global current_access_mode
    current_access_mode = AccessMode(args.access_mode)

    # Add the query tool with a description appropriate to the access mode
    if current_access_mode == AccessMode.UNRESTRICTED:
        mcp.add_tool(execute_sql, description="Execute any SQL query")
    else:
        mcp.add_tool(execute_sql, description="Execute a read-only SQL query")

    logger.info(f"Starting PostgreSQL MCP Server in {current_access_mode.upper()} mode")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
        )

    # Initialize database connection pool
    try:
        await db_connection.pool_connect(database_url)
        logger.info("Successfully connected to database and initialized connection pool")
        
        # --- MemoryDB recovery heartbeat: prove liveness, then recover uncommitted work left by a prior crash/reboot ---
        try:
            import os as _os
            await emit_component_heartbeat(HEARTBEAT_COMPONENT,
                {"state": "startup", "host": _os.environ.get("HOSTNAME", ""), "pid": _os.getpid()})
        except Exception as _hb_err:
            logger.warning("startup heartbeat failed: %s", _hb_err)
        try:
            _rec = await run_uncommitted_recovery(reason="startup")
            if _rec.get("recovered"):
                logger.info("startup recovery completed uncommitted work: %s", _rec)
            else:
                logger.info("startup recovery: %s", _rec)
        except Exception as _rec_err:
            logger.warning("startup recovery sweep failed: %s", _rec_err)
        
        # Auto-probe LLM endpoints
        try:
            logger.info("Probing LLM endpoints for context detection...")
            for pe, pt in DEFAULT_LLM_ENDPOINTS:
                try:
                    r = await scan_llm_endpoint(pe, pt)
                    logger.info("  %s: %s ctx=%s found=%s", pe, r["serving_system"], r["context_window"], r["found"])
                except Exception as pe2:
                    logger.warning("  %s: probe failed (%s)", pe, pe2)
            logger.info("LLM endpoint probing complete")
        except Exception as e:
            logger.warning("LLM probe skipped: %s", e)
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    # Set up proper shutdown handling
    try:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        # Windows doesn't support signals properly
        logger.warning("Signal handling not supported on Windows")
        pass

    # Run the server with the selected transport (always async)
    if args.transport == "stdio":
        await mcp.run_stdio_async()
    else:
        # Update FastMCP settings based on command line arguments.
        # The llama.cpp Web UI cannot initialize SSE MCP servers through its
        # CORS proxy because the SSE transport advertises a relative /messages
        # endpoint. Enable browser CORS here so web clients can connect directly.
        mcp.settings.host = args.sse_host
        mcp.settings.port = args.sse_port
        starlette_app = mcp.sse_app()
        for route in MEMORYDB_HTTP_ROUTES:
            starlette_app.routes.append(route)
        starlette_app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        guarded_app = StaleSseInitializationGuard(starlette_app)
        config = uvicorn.Config(
            guarded_app,
            host=mcp.settings.host,
            port=mcp.settings.port,
            log_level=mcp.settings.log_level.lower(),
        )
        server = uvicorn.Server(config)
        await server.serve()


async def shutdown(sig=None):
    """Clean shutdown of the server."""
    global shutdown_in_progress

    if shutdown_in_progress:
        logger.warning("Forcing immediate exit")
        # Use sys.exit instead of os._exit to allow for proper cleanup
        sys.exit(1)

    shutdown_in_progress = True

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    # Close database connections
    try:
        await db_connection.close()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    sys.exit(128 + sig if sig is not None else 0)
