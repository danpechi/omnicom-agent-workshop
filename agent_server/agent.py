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

SUPERVISOR_ENDPOINT = os.getenv("SUPERVISOR_ENDPOINT", "")  # e.g. "mas-881d67b9-endpoint"
DATABRICKS_HOST     = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")

logger.info("Supervisor endpoint: %s", SUPERVISOR_ENDPOINT or "(none — set via UI)")


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


def _get_token() -> str:
    """Get a bearer token that works for both PAT and OAuth/workload identity auth."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        headers: dict = {}
        w.config.authenticate(headers)
        auth = headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:]
        # fallback to explicit token if set
        return w.config.token or os.getenv("DATABRICKS_TOKEN", "")
    except Exception as e:
        logger.warning("Could not retrieve Databricks token: %s", e)
        return os.getenv("DATABRICKS_TOKEN", "")


def _sync_call_supervisor(question: str, endpoint: str) -> str:
    """Call the Supervisor Agent endpoint directly via the Responses API."""
    from openai import OpenAI
    client = OpenAI(
        api_key=_get_token(),
        base_url=f"{DATABRICKS_HOST}/serving-endpoints",
    )
    response = client.responses.create(
        model=endpoint,
        input=[{"role": "user", "content": question}],
    )
    return " ".join(
        getattr(content, "text", "")
        for output in response.output
        for content in getattr(output, "content", [])
    )


async def _answer(question: str, endpoint: str) -> str:
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_call_supervisor, question, endpoint)


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    question = _extract_question(request)
    endpoint = (request.custom_inputs or {}).get("supervisor_endpoint", "") or SUPERVISOR_ENDPOINT
    answer = await _answer(question, endpoint)

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
    endpoint = (request.custom_inputs or {}).get("supervisor_endpoint", "") or SUPERVISOR_ENDPOINT
    answer = await _answer(question, endpoint)

    yield ResponsesAgentStreamEvent.model_validate({
        "type": "response.output_item.done",
        "item": {
            "id": f"msg_{uuid.uuid4().hex[:8]}",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": answer}],
        },
    })
