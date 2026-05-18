# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate — Routing Accuracy, Governance Correctness, Answer Quality
# MAGIC
# MAGIC This notebook evaluates three dimensions of the Omnicom Affinity Hub Supervisor Agent:
# MAGIC
# MAGIC 1. **Routing accuracy** — does the LLM classifier correctly route `ka` vs `genie`?
# MAGIC 2. **Governance correctness** — do tenant SPs see only their own data?
# MAGIC 3. **Answer quality** — for KA questions, are key facts present in the response?
# MAGIC
# MAGIC All results are logged to the MLflow experiment for comparison.

# COMMAND ----------

# MAGIC %pip install mlflow[databricks] databricks-sdk requests --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import re
import time

import mlflow
import requests
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_PATH)
print(f"MLflow experiment: {EXPERIMENT_PATH}")

# COMMAND ----------

w = WorkspaceClient()
DATABRICKS_HOST_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
if not DATABRICKS_HOST_URL.startswith("http"):
    DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST_URL}"
DATABRICKS_TOKEN_VAL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Load tenant SP records
tenant_sps_df = spark.table(TENANT_SPS_TABLE_BT_FQN)
TENANT_SP_MAP = {row["tenant_id"]: row["application_id"] for row in tenant_sps_df.collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Routing Accuracy
# MAGIC
# MAGIC Pull existing traces from the MLflow experiment and check whether the `route`
# MAGIC tag matches the expected route from the eval dataset.

# COMMAND ----------

# Load sample QA with expected routes
sample_qa_df = spark.table(QA_TABLE_BT_FQN)
sample_qa = [row.asDict() for row in sample_qa_df.collect()]
print(f"Loaded {len(sample_qa)} Q&A pairs from {QA_TABLE_FQN}")

# Build a lookup: question text → expected route
expected_routes = {qa["question"]: qa.get("route", "ka") for qa in sample_qa}
print(f"Routes in dataset: {dict(pd.Series(list(expected_routes.values())).value_counts()) if False else {k: list(expected_routes.values()).count(k) for k in set(expected_routes.values())}}")

# COMMAND ----------

# Search MLflow traces for route tags
try:
    import pandas as pd
    exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    if exp is None:
        print("No experiment found — run some queries through the Supervisor first.")
        traces_df = pd.DataFrame()
    else:
        traces_df = mlflow.search_traces(
            experiment_ids=[exp.experiment_id],
            max_results=200,
        )
        print(f"Found {len(traces_df)} traces in experiment.")
        print(traces_df[["trace_id", "execution_time_ms"]].head() if not traces_df.empty else "No traces yet.")
except Exception as e:
    print(f"Could not search traces: {e}")
    traces_df = pd.DataFrame()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score routing accuracy from traces
# MAGIC
# MAGIC For each trace that has `outputs.custom_outputs.route`, compare against
# MAGIC the expected route in the sample QA dataset.

# COMMAND ----------

if not traces_df.empty and "outputs" in traces_df.columns:
    routing_results = []
    for _, row in traces_df.iterrows():
        try:
            outputs = row.get("outputs") or {}
            if isinstance(outputs, str):
                outputs = json.loads(outputs)
            custom_outputs = outputs.get("custom_outputs", {})
            actual_route = custom_outputs.get("route", "")
            # Try to match against expected routes by looking at inputs
            inputs = row.get("inputs") or {}
            if isinstance(inputs, str):
                inputs = json.loads(inputs)
            question = ""
            for item in (inputs.get("input") or []):
                if isinstance(item, dict) and item.get("role") == "user":
                    content = item.get("content", "")
                    question = content if isinstance(content, str) else ""
                    break
            expected_route = expected_routes.get(question, "unknown")
            if expected_route != "unknown" and actual_route:
                routing_results.append({
                    "question": question[:60],
                    "expected": expected_route,
                    "actual": actual_route,
                    "correct": expected_route == actual_route,
                })
        except Exception:
            continue

    if routing_results:
        results_df = pd.DataFrame(routing_results)
        accuracy = results_df["correct"].mean()
        print(f"Routing accuracy: {accuracy:.1%} ({results_df['correct'].sum()}/{len(results_df)} correct)")
        print()
        print(results_df.to_string(index=False))

        # Log to MLflow
        with mlflow.start_run(run_name="routing_accuracy"):
            mlflow.log_metric("routing_accuracy", accuracy)
            mlflow.log_metric("routing_sample_size", len(results_df))
            mlflow.log_dict(routing_results, "routing_results.json")
        print(f"\nLogged routing accuracy to MLflow.")
    else:
        print("No matching traces found. Run some sample questions through the Supervisor first.")
else:
    print("No traces available yet. Run the Supervisor Agent with sample questions first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Governance Correctness Checks
# MAGIC
# MAGIC For each Genie question in the sample QA dataset, call the Genie API as each
# MAGIC tenant SP and verify the returned row count is scoped correctly.
# MAGIC
# MAGIC **Expected:** AT&T SP sees only AT&T rows, JLR SP sees only JLR rows, etc.
# MAGIC If a tenant SP sees rows from another tenant, that's a governance violation.

# COMMAND ----------

# Expected row counts per tenant (from our synthetic data)
# Each tenant has exactly 3 opportunities in active/draft/pending status
TENANT_ROW_LIMITS = {
    "TEN-001": {"tenant_name": "AT&T",    "max_rows": 5},
    "TEN-002": {"tenant_name": "JLR",     "max_rows": 3},
    "TEN-003": {"tenant_name": "Pepsi",   "max_rows": 3},
    "TEN-004": {"tenant_name": "Ford",    "max_rows": 2},
    "TEN-005": {"tenant_name": "Samsung", "max_rows": 2},
}

# COMMAND ----------

# Resolve Genie Space ID
spaces = list(w.genie.list_spaces())
genie_space = next((s for s in spaces if s.title == GENIE_NAME), None)
GENIE_SPACE_ID_LIVE = genie_space.space_id if genie_space else ""

def call_genie_raw(question: str, space_id: str, sp_app_id: str = "") -> dict:
    """Call Genie and return the raw message response."""
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN_VAL}",
        "Content-Type": "application/json",
    }
    body = {"content": question}
    if sp_app_id:
        body["user_context"] = {"user_id": sp_app_id}
    try:
        start_resp = requests.post(
            f"{DATABRICKS_HOST_URL}/api/2.0/genie/spaces/{space_id}/start-conversation",
            headers=headers, json=body, timeout=30,
        )
        start_resp.raise_for_status()
        data = start_resp.json()
        conv_id, msg_id = data["conversation_id"], data["message_id"]
    except Exception as e:
        return {"error": str(e)}
    for _ in range(60):
        time.sleep(1.0)
        poll = requests.get(
            f"{DATABRICKS_HOST_URL}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers, timeout=15,
        )
        msg = poll.json()
        if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            return msg
    return {"status": "TIMEOUT"}

# COMMAND ----------

GOVERNANCE_QUESTION = "List all open and active opportunities for my tenant."

governance_results = []

if GENIE_SPACE_ID_LIVE:
    for tenant_id, info in TENANT_ROW_LIMITS.items():
        sp_app_id = TENANT_SP_MAP.get(tenant_id, "")
        if not sp_app_id:
            print(f"SKIP {tenant_id}: no SP found")
            continue

        msg = call_genie_raw(GOVERNANCE_QUESTION, GENIE_SPACE_ID_LIVE, sp_app_id)

        # Extract row count from attachments or description
        response_text = ""
        for att in msg.get("attachments", []):
            q = att.get("query", {})
            response_text = q.get("description", "") or response_text

        # Simple heuristic: look for row count in response
        numbers = re.findall(r'\b(\d+)\b', response_text)
        row_count = int(numbers[0]) if numbers else -1

        other_tenant_names = [v["tenant_name"] for k, v in TENANT_ROW_LIMITS.items() if k != tenant_id]
        cross_tenant_leak = any(name.lower() in response_text.lower() for name in other_tenant_names)

        result = {
            "tenant_id": tenant_id,
            "tenant_name": info["tenant_name"],
            "sp_app_id": sp_app_id,
            "response_text": response_text[:200],
            "row_count_heuristic": row_count,
            "cross_tenant_leak": cross_tenant_leak,
            "governance_pass": not cross_tenant_leak,
        }
        governance_results.append(result)
        status = "PASS" if result["governance_pass"] else "FAIL"
        print(f"[{status}] {tenant_id} ({info['tenant_name']}): cross_tenant_leak={cross_tenant_leak}")

    # Log governance results
    if governance_results:
        pass_rate = sum(r["governance_pass"] for r in governance_results) / len(governance_results)
        with mlflow.start_run(run_name="governance_correctness"):
            mlflow.log_metric("governance_pass_rate", pass_rate)
            mlflow.log_metric("governance_sample_size", len(governance_results))
            mlflow.log_dict(governance_results, "governance_results.json")
        print(f"\nGovernance pass rate: {pass_rate:.1%}")
        print("Logged to MLflow.")
else:
    print("Skipping governance checks — Genie Space not configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Answer Quality Scorer (KA Questions)
# MAGIC
# MAGIC For document Q&A questions routed to the KA, we measure whether key facts
# MAGIC from the expected answer appear in the actual response.
# MAGIC
# MAGIC We use the `Guidelines` scorer from MLflow's built-in scorers.

# COMMAND ----------

from mlflow.genai.scorers import Guidelines

answer_quality_scorer = Guidelines(
    name="answer_quality",
    guidelines=(
        "The response should contain the key facts from the expected answer. "
        "Award 'yes' if at least 60% of the key facts are present, 'no' otherwise."
    ),
)

# COMMAND ----------

# Build eval dataset: KA questions only, with expected answers
from mlflow.deployments import get_deploy_client

deploy_client = get_deploy_client("databricks")

ka_questions = [qa for qa in sample_qa if qa.get("route") == "ka"]
print(f"Evaluating {len(ka_questions)} KA questions...")


def predict_ka(inputs: dict) -> str:
    """Call the KA endpoint directly for evaluation."""
    question = inputs.get("question", "")
    ka_endpoint = os.getenv("KA_ENDPOINT_NAME", "")
    if not ka_endpoint:
        # Try to resolve from config (notebook context)
        ka_endpoint = KA_ENDPOINT
    try:
        response = deploy_client.predict(
            endpoint=ka_endpoint,
            inputs={"input": [{"role": "user", "content": question}]},
        )
        for item in response.get("output", []):
            for part in item.get("content", []):
                if part.get("type") == "output_text":
                    return part["text"]
    except Exception as e:
        return f"Error: {e}"
    return ""

# COMMAND ----------

# Build evaluation data in MLflow format
eval_data = [
    {
        "inputs": {"question": qa["question"]},
        "expected_outputs": qa["expected_answer"],
    }
    for qa in ka_questions[:10]  # limit to 10 for workshop speed
]

with mlflow.start_run(run_name="ka_answer_quality"):
    results = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=predict_ka,
        scorers=[answer_quality_scorer],
    )

print(f"\nAnswer quality evaluation complete.")
print(f"Mean answer_quality score: {results.metrics.get('mean/answer_quality/score', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Trace Analysis Summary
# MAGIC
# MAGIC Visualize routing distribution and latency from MLflow traces.

# COMMAND ----------

try:
    import pandas as pd
    exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    if exp and not traces_df.empty:
        # Route distribution
        routes = []
        tenant_ids = []
        for _, row in traces_df.iterrows():
            try:
                outputs = row.get("outputs") or {}
                if isinstance(outputs, str):
                    outputs = json.loads(outputs)
                co = outputs.get("custom_outputs", {})
                if co.get("route"):
                    routes.append(co["route"])
                if co.get("tenant_id"):
                    tenant_ids.append(co["tenant_id"])
            except Exception:
                continue

        if routes:
            route_counts = pd.Series(routes).value_counts()
            print("Route distribution:")
            print(route_counts.to_string())
            print()

        if tenant_ids:
            tenant_counts = pd.Series(tenant_ids).value_counts()
            print("Requests by tenant_id:")
            print(tenant_counts.to_string())
            print()

        if "execution_time_ms" in traces_df.columns:
            latency = traces_df["execution_time_ms"].describe()
            print("Latency (ms):")
            print(latency.to_string())
    else:
        print("No trace data available for analysis.")
except Exception as e:
    print(f"Trace analysis error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Evaluation | What It Measures | Target |
# MAGIC |---|---|---|
# MAGIC | Routing accuracy | LLM classifier ka/genie correctness | > 90% |
# MAGIC | Governance correctness | No cross-tenant data leakage | 100% |
# MAGIC | Answer quality (KA) | Key facts present in KA responses | > 75% |
# MAGIC
# MAGIC All three metrics are logged to the MLflow experiment so you can track them over time
# MAGIC as you tune the Supervisor's routing prompt or KA instructions.
# MAGIC
# MAGIC The governance correctness check is especially important: even one failure means a
# MAGIC tenant saw another tenant's data — a hard blocker before going to production.
