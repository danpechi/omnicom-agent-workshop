"""Omnicom Affinity Hub — Supervisor Agent using Databricks Responses API."""

import logging
import os
import uuid
from typing import Any, AsyncGenerator

import mlflow
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

logger = logging.getLogger(__name__)
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)

_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME")
if _EXPERIMENT_NAME:
    try:
        mlflow.set_experiment(_EXPERIMENT_NAME)
    except Exception as e:
        logger.warning("Could not set MLflow experiment %s: %s", _EXPERIMENT_NAME, e)

SUPERVISOR_DISPLAY_NAME = os.getenv("SUPERVISOR_DISPLAY_NAME", "")  # e.g. "dan-pechi-adtech-supervisor"
DATABRICKS_HOST         = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
LLM_ENDPOINT            = os.getenv("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")


def _resolve_supervisor_resource_name() -> str:
    """Resolve SUPERVISOR_DISPLAY_NAME → resource name (supervisor-agents/{id})."""
    if not SUPERVISOR_DISPLAY_NAME:
        return ""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        resp = w.api_client.do("GET", "/api/2.1/supervisor-agents")
        for sa in resp.get("supervisor_agents", []):
            if sa.get("display_name") == SUPERVISOR_DISPLAY_NAME:
                return sa["name"]
        logger.warning("Supervisor Agent '%s' not found in workspace.", SUPERVISOR_DISPLAY_NAME)
    except Exception as e:
        logger.warning("Could not resolve Supervisor Agent name: %s", e)
    return ""


_SA_RESOURCE_NAME: str = _resolve_supervisor_resource_name()

_SUPERVISOR_TOOLS: list[dict[str, Any]] = []
if _SA_RESOURCE_NAME:
    _SUPERVISOR_TOOLS.append({
        "type": "supervisor_agent",
        "supervisor_agent": {"name": _SA_RESOURCE_NAME},
    })

logger.info("Supervisor tools: %s tool(s), SA=%s", len(_SUPERVISOR_TOOLS), _SA_RESOURCE_NAME or "(none)")


def _build_trace_url(trace_id: str) -> str | None:
    if not trace_id or not DATABRICKS_HOST:
        return None
    host = DATABRICKS_HOST if DATABRICKS_HOST.startswith("http") else f"https://{DATABRICKS_HOST}"
    try:
        if _EXPERIMENT_NAME:
            exp = mlflow.get_experiment_by_name(_EXPERIMENT_NAME)
            if exp:
                return f"{host}/ml/experiments/{exp.experiment_id}/traces?selectedTraceId={trace_id}"
    except Exception:
        pass
    return f"{host}/ml/traces/{trace_id}"


def _capture_trace_id() -> str | None:
    try:
        span = mlflow.get_current_active_span()
        if span and hasattr(span, "trace_id") and span.trace_id:
            return span.trace_id
    except Exception:
        pass
    try:
        fn = getattr(mlflow, "get_last_active_trace_id", None)
        if callable(fn):
            trace_id = fn(thread_local=True)
            if trace_id:
                return trace_id
    except Exception:
        pass
    return None


def _extract_question(request: ResponsesAgentRequest) -> str:
    for item in reversed(request.input):
        d = item.model_dump()
        if d.get("role") == "user":
            content = d.get("content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "input_text":
                        return part.get("text", "")
    return ""


def _build_tools(supervisor_name: str) -> list[dict[str, Any]]:
    """Return supervisor_agent tool list for a given resource name."""
    name = supervisor_name or _SA_RESOURCE_NAME
    if not name:
        return []
    return [{"type": "supervisor_agent", "supervisor_agent": {"name": name}}]


def _sync_call_supervisor(question: str, supervisor_name: str = "") -> str:
    """Call the Databricks Supervisor API (POST /mlflow/v1/responses)."""
    from databricks_openai import DatabricksOpenAI
    client = DatabricksOpenAI(use_ai_gateway=True)
    response = client.responses.create(
        model=LLM_ENDPOINT,
        input=[{"type": "message", "role": "user", "content": question}],
        tools=_build_tools(supervisor_name),
        stream=False,
    )
    return response.output_text or ""


async def _answer(question: str, supervisor_name: str = "") -> str:
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_call_supervisor, question, supervisor_name)


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    question = _extract_question(request)
    supervisor_name = (request.custom_inputs or {}).get("supervisor_name", "")
    answer = await _answer(question, supervisor_name)

    output_item = {
        "id": f"msg_{uuid.uuid4().hex[:8]}",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": answer}],
    }

    custom_outputs: dict[str, Any] = {}
    try:
        trace_id = _capture_trace_id()
        if trace_id:
            custom_outputs["trace_id"] = trace_id
            url = _build_trace_url(trace_id)
            if url:
                custom_outputs["trace_url"] = url
        if _EXPERIMENT_NAME:
            custom_outputs["experiment_name"] = _EXPERIMENT_NAME
    except Exception as e:
        logger.warning("trace metadata capture failed: %s", e)

    return ResponsesAgentResponse.model_validate({"output": [output_item], "custom_outputs": custom_outputs})


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    question = _extract_question(request)
    supervisor_name = (request.custom_inputs or {}).get("supervisor_name", "")
    answer = await _answer(question, supervisor_name)

    yield ResponsesAgentStreamEvent.model_validate({
        "type": "response.output_item.done",
        "item": {
            "id": f"msg_{uuid.uuid4().hex[:8]}",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": answer}],
        },
    })
