"""Omnicom Affinity Hub — Supervisor Agent using Databricks Responses API."""

import logging
import os
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

KA_ENDPOINT_NAME  = os.getenv("KA_ENDPOINT_NAME", "")   # KA serving endpoint name
GENIE_SPACE_NAME  = os.getenv("GENIE_SPACE_NAME", "")   # Genie Space title
DATABRICKS_HOST   = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
LLM_ENDPOINT      = os.getenv("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")


def _resolve_ka_tile_id() -> str:
    """Resolve KA endpoint name → tile ID via the Knowledge Assistants API."""
    if not KA_ENDPOINT_NAME:
        return ""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        resp = w.api_client.do("GET", "/api/2.1/knowledge-assistants")
        for ka in resp.get("knowledge_assistants", []):
            if ka.get("endpoint_name") == KA_ENDPOINT_NAME:
                return ka["id"]
    except Exception as e:
        logger.warning("Could not resolve KA tile ID for endpoint %s: %s", KA_ENDPOINT_NAME, e)
    return ""


def _resolve_genie_space_id() -> str:
    """Resolve GENIE_SPACE_NAME to a space ID via the Genie API."""
    if not GENIE_SPACE_NAME:
        return ""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        spaces = resp.get("genie_spaces", []) if isinstance(resp, dict) else []
        match = next((s for s in spaces if s.get("title") == GENIE_SPACE_NAME), None)
        if match:
            return match["space_id"]
    except Exception as e:
        logger.warning("Could not resolve Genie Space name %s: %s", GENIE_SPACE_NAME, e)
    return ""


# Resolve both IDs once at startup
_KA_TILE_ID: str = _resolve_ka_tile_id()
_GENIE_SPACE_ID: str = _resolve_genie_space_id()

_SUPERVISOR_TOOLS: list[dict[str, Any]] = []
if _KA_TILE_ID:
    _SUPERVISOR_TOOLS.append({
        "type": "knowledge_assistant",
        "knowledge_assistant": {
            "knowledge_assistant_id": _KA_TILE_ID,
            "description": (
                "Answers questions about methodology, playbooks, onboarding procedures, "
                "account information, case studies, and campaign guidelines from documents."
            ),
        },
    })
if _GENIE_SPACE_ID:
    _SUPERVISOR_TOOLS.append({
        "type": "genie_space",
        "genie_space": {
            "id": _GENIE_SPACE_ID,
            "description": (
                "Answers questions about campaign performance, financials, client data, "
                "creative assets, and any question that requires querying structured data."
            ),
        },
    })

logger.info("Supervisor tools: %s", [t["type"] for t in _SUPERVISOR_TOOLS])


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


def _sync_call_supervisor(question: str) -> str:
    """Call the Databricks Supervisor API (POST /mlflow/v1/responses)."""
    from databricks_openai import DatabricksOpenAI
    client = DatabricksOpenAI(use_ai_gateway=True)
    response = client.responses.create(
        model=LLM_ENDPOINT,
        input=[{"type": "message", "role": "user", "content": question}],
        tools=_SUPERVISOR_TOOLS,
        stream=False,
    )
    return response.output_text or ""


async def _answer(question: str) -> str:
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_call_supervisor, question)


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    question = _extract_question(request)
    answer = await _answer(question)

    output_item = {
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
    answer = await _answer(question)

    yield ResponsesAgentStreamEvent.model_validate({
        "type": "response.output_item.done",
        "item": {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": answer}],
        },
    })
