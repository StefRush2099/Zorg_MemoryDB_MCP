"""HTTP backend for native OpenClaw tools, using the existing MemoryDB code.

Includes a recovery worker that polls memory_llm_job_queue for pending
zorg-recovery-* jobs, calls the last active LLM worker, and commits the
result into lan_chat_messages and zorg_turn_heartbeat.
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import urllib.error
import urllib.request
from contextlib import asynccontextmanager

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

logger = logging.getLogger("memorydb-plugin")


# ---------------------------------------------------------------------------
# Recovery worker: polls memory_llm_job_queue for pending zorg-recovery-*
# jobs, calls the LLM endpoint, and commits the result.
# ---------------------------------------------------------------------------

async def _recovery_worker(db_uri: str, poll_interval: float = 5.0):
    """Background task: consume pending zorg-recovery jobs."""
    import asyncpg

    logger.info("Recovery worker started (poll=%ss)", poll_interval)
    pool = await asyncpg.create_pool(db_uri, min_size=1, max_size=3)

    while True:
        try:
            async with pool.acquire() as conn:
                jobs = await conn.fetch(
                    """
                    SELECT queue_id, job_key, payload_snapshot
                    FROM memory_llm_job_queue
                    WHERE status = 'pending'
                      AND job_key LIKE 'zorg-recovery-%%'
                    ORDER BY created_at ASC
                    LIMIT 3
                    FOR UPDATE SKIP LOCKED
                    """
                )

            for job in jobs:
                queue_id = job["queue_id"]
                job_key = job["job_key"]
                payload = job["payload_snapshot"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                turn_id = payload.get("turn_id", "")
                session_key = payload.get("session_key", "agent:main:lan-chat")
                llm_endpoint = payload.get("llm_endpoint", "").rstrip("/")
                llm_model = payload.get("llm_model", "")
                recovery_prompt = payload.get("recovery_prompt", "")

                if not llm_endpoint or not recovery_prompt:
                    logger.warning("Recovery job %s missing endpoint or prompt", job_key)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE memory_llm_job_queue SET status='failed', error_text=$1, finished_at=now() WHERE queue_id=$2",
                            "missing endpoint or prompt", queue_id,
                        )
                    continue

                # Determine API format
                api_format = "ollama_api"
                if not llm_endpoint.endswith("/api/chat"):
                    # Try openai-compatible probe
                    try:
                        req = urllib.request.Request(llm_endpoint + "/v1/models")
                        with urllib.request.urlopen(req, timeout=5) as resp:
                            data = json.loads(resp.read().decode())
                            if "data" in data:
                                api_format = "openai_compatible"
                    except Exception:
                        pass

                # Build and send request
                try:
                    content = await asyncio.to_thread(
                        _call_llm, llm_endpoint, llm_model, recovery_prompt, api_format
                    )
                    if not content:
                        raise ValueError("Empty LLM response")

                    # Commit recovered response
                    metadata = {
                        "kind": "assistant_final",
                        "source": "zorg_turn_recovery",
                        "recovered_turn_id": turn_id,
                        "llm_endpoint": llm_endpoint,
                        "llm_model": llm_model,
                    }
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """INSERT INTO public.lan_chat_messages
                               (session_key, role, content, metadata)
                               VALUES ($1, 'assistant', $2, $3::jsonb)""",
                            session_key, content, json.dumps(metadata),
                        )
                        await conn.execute(
                            """UPDATE memory_llm_job_queue
                               SET status='done', finished_at=now(), result_summary=$1
                               WHERE queue_id=$2""",
                            content[:500], queue_id,
                        )
                        await conn.execute(
                            """UPDATE zorg_turn_heartbeat
                               SET state='recovered', committed_at=now(), recovered_at=now(),
                                   metadata = metadata || $1::jsonb
                               WHERE turn_id=$2""",
                            json.dumps({"recovery_status": "success"}), turn_id,
                        )
                    logger.info("Recovery job %s completed (turn=%s, %d chars)",
                               job_key, turn_id, len(content))

                except Exception as e:
                    error_msg = str(e)[:500]
                    logger.error("Recovery job %s failed: %s", job_key, error_msg)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """UPDATE memory_llm_job_queue
                               SET status='failed', finished_at=now(), error_text=$1,
                                   attempts = attempts + 1
                               WHERE queue_id=$2""",
                            error_msg, queue_id,
                        )
                        await conn.execute(
                            """UPDATE zorg_turn_heartbeat
                               SET state='recovery_failed',
                                   metadata = metadata || $1::jsonb
                               WHERE turn_id=$2""",
                            json.dumps({"error": error_msg}), turn_id,
                        )

        except Exception as e:
            logger.error("Recovery worker loop error: %s", e)

        await asyncio.sleep(poll_interval)


def _call_llm(endpoint: str, model: str, prompt: str, api_format: str,
              timeout: int = 120) -> str:
    """Synchronous LLM call (runs in thread via asyncio.to_thread)."""
    messages = [
        {"role": "system", "content": "You are Zorg, recovering from a system interruption. Provide a complete, concise response."},
        {"role": "user", "content": prompt},
    ]

    if api_format == "ollama_api":
        url = endpoint if endpoint.endswith("/api/chat") else endpoint + "/api/chat"
        body = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 4096},
        }
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        msg = data.get("message") or {}
        return str(msg.get("content") or msg.get("reasoning") or "").strip()
    else:
        url = endpoint + "/v1/chat/completions"
        body = {
            "model": model,
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 4096,
        }
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        return data["choices"][0]["message"]["content"].strip()


# ---------------------------------------------------------------------------
# HTTP backend (original)
# ---------------------------------------------------------------------------

def encode(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    return value


def result_body(value):
    if isinstance(value, tuple) and len(value) == 2:
        return {"content": encode(value[0]), "structuredContent": encode(value[1])}
    value = encode(value)
    if isinstance(value, list):
        return {"content": value}
    if isinstance(value, dict) and "content" in value:
        return value
    return {
        "content": [{"type": "text", "text": json.dumps(value)}],
        "structuredContent": value,
    }


async def create_app(backend, database_uri, token, access_mode="unrestricted"):
    if not database_uri or not token:
        raise RuntimeError("DATABASE_URI and POSTGRES_NATIVE_TOKEN must be set")

    backend.current_access_mode = backend.AccessMode(access_mode)
    description = (
        "Execute any SQL query" if access_mode == "unrestricted"
        else "Execute a read-only SQL query"
    )
    backend.mcp.add_tool(backend.execute_sql, description=description)
    catalog = sorted(
        encode(await backend.mcp.list_tools()), key=lambda tool: tool["name"]
    )
    names = {tool["name"] for tool in catalog}
    fingerprint = hashlib.sha256(
        json.dumps(catalog, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    expected_auth = f"Bearer {token}".encode()

    @asynccontextmanager
    async def lifespan(_app):
        try:
            await backend.db_connection.pool_connect(database_uri)
            # Start recovery worker in background
            recovery_task = asyncio.create_task(_recovery_worker(database_uri))
            logger.info("Started recovery worker task")
            yield
            recovery_task.cancel()
            try:
                await recovery_task
            except asyncio.CancelledError:
                pass
        finally:
            await backend.db_connection.close()

    async def handle(request):
        supplied = request.headers.get("authorization", "").encode()
        if not hmac.compare_digest(supplied, expected_auth):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

        if request.method == "GET":
            return JSONResponse({"tools": catalog, "fingerprint": fingerprint})

        if request.headers.get("x-tool-catalog") != fingerprint:
            return JSONResponse(
                {"error": "Tool catalog changed; refresh the plugin catalog."},
                status_code=409,
            )

        name = request.path_params["name"]
        if name not in names:
            return JSONResponse({"error": "Unknown tool"}, status_code=404)

        try:
            arguments = await request.json()
        except (ValueError, UnicodeDecodeError):
            return JSONResponse(
                {"error": "Expected JSON arguments"}, status_code=400,
            )

        if not isinstance(arguments, dict):
            return JSONResponse(
                {"error": "Arguments must be an object"}, status_code=400,
            )

        try:
            result = await backend.mcp.call_tool(name, arguments)
            return JSONResponse(result_body(result))
        except Exception as error:
            return JSONResponse({
                "isError": True,
                "content": [{"type": "text", "text": str(error)}],
            })

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route("/tools", handle, methods=["GET"]),
            Route("/tools/{name}", handle, methods=["POST"]),
        ],
    )


async def main():
    from postgres_mcp import server as backend

    parser = argparse.ArgumentParser(
        description="MemoryDB OpenClaw HTTP backend"
    )
    parser.add_argument(
        "--access-mode",
        choices=["unrestricted", "restricted"],
        default="unrestricted",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    app = await create_app(
        backend,
        os.environ.get("DATABASE_URI"),
        os.environ.get("POSTGRES_NATIVE_TOKEN"),
        args.access_mode,
    )

    await uvicorn.Server(
        uvicorn.Config(
            app,
            host=args.host,
            port=args.port,
            access_log=False,
        )
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
