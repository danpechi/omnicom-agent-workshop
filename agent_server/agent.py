"""Omnicom Affinity Hub — Supervisor Agent routing questions to KA or Genie."""

import logging
import os
import time
from typing import Any, AsyncGenerator

import mlflow
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)


logger = logging.getLogger(__name__)
mlflow.langchain.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)

_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME")
if _EXPERIMENT_NAME:
    try:
        mlflow.set_experiment(_EXPERIMENT_NAME)
        logger.info("MLflow experiment pinned to %s", _EXPERIMENT_NAME)
    except Exception as e:
        logger.warning("Could not set MLflow experiment %s: %s", _EXPERIMENT_NAME, e)

KA_ENDPOINT_NAME  = os.getenv("KA_ENDPOINT_NAME", "")
GENIE_SPACE_NAME  = os.getenv("GENIE_SPACE_NAME", "")
PROMPT_ALIAS      = os.getenv("AGENT_PROMPT_VERSION", "v1")
DATABRICKS_HOST   = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN", "")
LLM_ENDPOINT      = os.getenv("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")

# Routing system prompt — keep it short so the classification call is fast
_ROUTING_PROMPT = """\
You are a routing agent for the Omnicom Affinity Hub assistant.
Classify the user's question as one of two types:

- "genie" — the question asks about data, metrics, counts, distributions, specific records,
  opportunity pipeline status, campaign performance numbers, or anything that requires
  querying a database or structured table.

- "ka" — the question asks about methodology, procedures, guidelines, how-to, account
  information, case studies, onboarding steps, pricing, or anything answered from documents.

Respond with ONLY one word: genie  or  ka"""


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


def _sync_route(question: str) -> str:
    """Call the LLM to classify the question. Returns 'genie' or 'ka'."""
    from mlflow.deployments import get_deploy_client
    client = get_deploy_client("databricks")
    try:
        resp = client.predict(
            endpoint=LLM_ENDPOINT,
            inputs={
                "messages": [
                    {"role": "system", "content": _ROUTING_PROMPT},
                    {"role": "user", "content": question},
                ],
                "max_tokens": 5,
                "temperature": 0,
            },
        )
        label = resp["choices"][0]["message"]["content"].strip().lower()
        return "genie" if "genie" in label else "ka"
    except Exception as e:
        logger.warning("Routing LLM call failed (%s), defaulting to ka", e)
        return "ka"


def _resolve_genie_space_id() -> str:
    """Resolve GENIE_SPACE_NAME to a space ID via the Genie API."""
    if not GENIE_SPACE_NAME:
        return ""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        spaces = list(w.genie.list_spaces())
        match = next((s for s in spaces if s.title == GENIE_SPACE_NAME), None)
        if match:
            return match.space_id
    except Exception as e:
        logger.warning("Could not resolve Genie Space name %s: %s", GENIE_SPACE_NAME, e)
    return ""


def _sync_call_genie(question: str, space_id: str) -> str:
    """Call the Genie Conversation API synchronously."""
    import requests

    if not space_id:
        return "Genie Space is not configured. Please set GENIE_SPACE_NAME in app.yaml."

    host = DATABRICKS_HOST if DATABRICKS_HOST.startswith("http") else f"https://{DATABRICKS_HOST}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    # Start conversation
    try:
        start = requests.post(
            f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
            headers=headers,
            json={"content": question},
            timeout=30,
        )
        start.raise_for_status()
        data = start.json()
        conv_id = data["conversation_id"]
        msg_id  = data["message_id"]
    except Exception as e:
        return f"Failed to start Genie conversation: {e}"

    # Poll for result (max ~60 seconds)
    for _ in range(60):
        time.sleep(1.0)
        try:
            poll = requests.get(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
                headers=headers,
                timeout=15,
            )
            poll.raise_for_status()
            msg = poll.json()
        except Exception as e:
            return f"Genie polling error: {e}"

        status = msg.get("status", "")
        if status == "COMPLETED":
            # Prefer the natural-language description from the query attachment
            for att in msg.get("attachments", []):
                q = att.get("query", {})
                if q.get("description"):
                    return q["description"]
            return msg.get("content") or "Query returned no results."
        elif status in ("FAILED", "CANCELLED"):
            return f"Genie query failed: {msg.get('error', 'Unknown error')}"

    return "Genie query timed out (60s)."


def _sync_call_ka(question: str) -> str:
    """Call the KA serving endpoint."""
    if not KA_ENDPOINT_NAME:
        return "KA_ENDPOINT_NAME is not configured. Please set it in app.yaml."
    from mlflow.deployments import get_deploy_client
    client = get_deploy_client("databricks")
    response = client.predict(
        endpoint=KA_ENDPOINT_NAME,
        inputs={"input": [{"role": "user", "content": question}]},
    )
    for item in response.get("output", []):
        for part in item.get("content", []):
            if part.get("type") == "output_text":
                return part["text"]
    return str(response)


async def _route_and_answer(question: str) -> tuple[str, str]:
    """Returns (answer, route) where route is 'ka' or 'genie'."""
    import asyncio
    loop = asyncio.get_event_loop()

    route = await loop.run_in_executor(None, _sync_route, question)

    if route == "genie":
        space_id = await loop.run_in_executor(None, _resolve_genie_space_id)
        answer = await loop.run_in_executor(None, _sync_call_genie, question, space_id)
    else:
        answer = await loop.run_in_executor(None, _sync_call_ka, question)

    return answer, route


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    question = _extract_question(request)
    answer, route = await _route_and_answer(question)

    output_item = {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": answer}],
    }

    custom_outputs: dict[str, Any] = {"prompt_alias": PROMPT_ALIAS, "route": route}
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
    answer, _ = await _route_and_answer(question)

    yield ResponsesAgentStreamEvent.model_validate({
        "type": "response.output_item.done",
        "item": {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": answer}],
        },
    })
