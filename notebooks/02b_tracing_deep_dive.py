# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # KA Tracing & Evaluation
# MAGIC
# MAGIC Every query to the Omnicom Affinity Hub Knowledge Assistant generates a full MLflow trace —
# MAGIC automatically, with no instrumentation code required. This notebook shows you how to
# MAGIC find, read, and evaluate those traces at scale.
# MAGIC
# MAGIC **Objectives:**
# MAGIC 1. **Generate traces** — fire 5 synthetic AdTech questions at the KA endpoint
# MAGIC 2. **Explore traces** — programmatic search, latency diagnosis, span drilldown
# MAGIC 3. **Evaluate quality** — `mlflow.genai.evaluate` with a custom guideline scorer
# MAGIC 4. **Production observability** — snapshot traces to Delta, run SQL analytics

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install databricks-agents==1.6.0 "mlflow[databricks]>=3.5" databricks-openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Setup
import json
import mlflow
import os
from mlflow.deployments import get_deploy_client
from mlflow import MlflowClient

mlflow.set_experiment(EXPERIMENT_PATH)
os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"

# Resolve the actual KA serving endpoint name (auto-generated, e.g. ka-0e618699-endpoint)
# KA_ENDPOINT from config holds the KA display name; the API returns the real endpoint_name.
from databricks.sdk import WorkspaceClient as _WC
_w = _WC()
try:
    _ka_list = _w.api_client.do("GET", "/api/2.1/knowledge-assistants").get("knowledge_assistants", [])
    _ka = next((k for k in _ka_list if k.get("display_name") == KA_ENDPOINT or k.get("endpoint_name") == KA_ENDPOINT), None)
    if _ka and _ka.get("endpoint_name"):
        KA_ENDPOINT = _ka["endpoint_name"]
except Exception as _e:
    print(f"WARN: could not resolve KA endpoint name: {_e}")

ka_instruction = (
    "You are a helpful assistant for Omnicom Affinity Hub. "
    "Answer questions about AdTech campaign methodology, client onboarding, "
    "campaign performance, and strategic playbooks. "
    "Always cite the source document when possible."
)

print(f"Experiment : {EXPERIMENT_PATH}")
print(f"KA endpoint: {KA_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Generate Traces
# MAGIC
# MAGIC Five synthetic questions that cover the range of document topics in the KA:
# MAGIC methodology, onboarding, case studies, campaign operations, and performance review.

# COMMAND ----------

# DBTITLE 1,5 synthetic AdTech questions
TEST_QUESTIONS = [
    "What are the key brand positioning differences between AutoNova and FreshGlow based on their 2025 brand guidelines?",
    "How did the AutoNova Spring Launch campaign perform against its brief objectives, comparing the campaign brief to the post-campaign report?",
    "What media channels and budget allocations are outlined in the AutoNova Q2 2025 media plan, and how do they compare to HealthFirst's Q4 2024 plan?",
    "What were the main findings from the Hilton Value Segmentation research, and how do the qualitative and quantitative reports complement each other?",
    "What consumer trends from the 2026 Global Consumer Predictions report could inform future campaign strategies for brands like QuickBite or StreamPlay?",
]

# COMMAND ----------

# DBTITLE 1,Call KA and generate traces
client = get_deploy_client("databricks")

traces_generated = 0
for i, question in enumerate(TEST_QUESTIONS, 1):
    try:
        response = client.predict(
            endpoint=KA_ENDPOINT,
            inputs={"input": [{"role": "user", "content": question}]},
        )
        # KA returns Responses API format: output[].content[].text
        answer = ""
        for item in response.get("output", []):
            for part in item.get("content", []):
                if part.get("type") == "output_text":
                    answer = part["text"]
        print(f"[{i}/{len(TEST_QUESTIONS)}] Q: {question[:70]}...")
        print(f"           A: {answer[:120]}...")
        print()
        traces_generated += 1
    except Exception as e:
        print(f"[{i}/{len(TEST_QUESTIONS)}] ERROR: {e}")

print(f"Generated {traces_generated}/{len(TEST_QUESTIONS)} traces.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Traces in the MLflow UI
# MAGIC
# MAGIC 1. Go to **Machine Learning → Agents** in the left sidebar.
# MAGIC 2. Click your KA → **View traces**.
# MAGIC 3. Click any trace to see the full span waterfall:
# MAGIC
# MAGIC ```
# MAGIC ► knowledge_assistant          (AGENT span — root)
# MAGIC   ├── retrieve                 (RETRIEVER span — chunks fetched from volume)
# MAGIC   │     inputs:  {"query": "What is the Affinity Loop..."}
# MAGIC   │     outputs: [{"content": "...", "source": "methodology.md"}, ...]
# MAGIC   └── llm_generate             (LLM span — model call with context)
# MAGIC         inputs:  {"messages": [system+context, user question]}
# MAGIC         outputs: {"content": "The Affinity Loop is..."}
# MAGIC ```
# MAGIC
# MAGIC **What to look for:**
# MAGIC - **Retrieval span** — which chunks were returned (poor chunks = poor answers)
# MAGIC - **LLM span** — token counts (useful for cost monitoring)
# MAGIC - **Trace tags** — filter by `tags.source` or custom tags you add

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Programmatic Trace Search

# COMMAND ----------

# DBTITLE 1,Search and display recent traces
experiment = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
all_traces = mlflow.search_traces(
    experiment_ids=[experiment.experiment_id],
    max_results=50,
    order_by=["timestamp DESC"],
)

print(f"Found {len(all_traces)} traces\n")
print(f"  {'Trace name':<45} {'State':<6} {'Duration (ms)':>14}")
print(f"  {'-'*45} {'-'*6} {'-'*14}")
for _, row in all_traces.iterrows():
    tags = row.get("tags", {}) or {}
    name = tags.get("mlflow.traceName", "unknown")[:45]
    state = str(row.get("state", "?"))[:6]
    dur = row.get("execution_duration")
    dur_str = f"{int(dur):>12,}" if dur is not None else "           ?"
    print(f"  {name:<45} {state:<6} {dur_str} ms")

# COMMAND ----------

# DBTITLE 1,Find slow traces (latency outliers)
SLOW_THRESHOLD_MS = 3000

slow_traces = all_traces[
    all_traces["execution_duration"].fillna(0) > SLOW_THRESHOLD_MS
].sort_values("execution_duration", ascending=False)

print(f"Slow traces (> {SLOW_THRESHOLD_MS} ms): {len(slow_traces)}")
for _, row in slow_traces.iterrows():
    tags = row.get("tags", {}) or {}
    name = tags.get("mlflow.traceName", "unknown")
    dur = row.get("execution_duration", 0)
    print(f"  {name:<50}  {int(dur):>8,} ms  trace_id={row['trace_id'][:16]}...")

# COMMAND ----------

# DBTITLE 1,Drill into the slowest trace
if len(all_traces) > 0:
    mlflow_client = MlflowClient()
    slowest_row = all_traces.sort_values("execution_duration", ascending=False).iloc[0]
    trace_id = slowest_row["trace_id"]
    trace = mlflow_client.get_trace(trace_id)

    total_ms = slowest_row.get("execution_duration", 0) or 0
    print(f"Slowest trace : {trace_id}")
    print(f"  Duration    : {int(total_ms):,} ms")
    print(f"  Status      : {trace.info.status}")
    print(f"  Spans       : {len(trace.data.spans)}")
    print()

    def _span_ms(span):
        return ((span.end_time_ns or 0) - (span.start_time_ns or 0)) / 1e6

    print(f"  {'Type':<14} {'Span name':<40} {'Duration':>10}")
    print(f"  {'-'*14} {'-'*40} {'-'*10}")
    for span in sorted(trace.data.spans, key=lambda s: -_span_ms(s)):
        span_type = getattr(span, "span_type", "UNKNOWN")
        if hasattr(span_type, "value"):
            span_type = str(span_type.value)
        print(f"  {str(span_type)[:14]:<14} {span.name[:40]:<40} {_span_ms(span):>8.1f} ms")

    print()
    print("Top span = bottleneck:")
    print("  RETRIEVER slow → vector search or index issue")
    print("  LLM slow       → token count or endpoint cold start")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Evaluate Quality with a Custom Scorer
# MAGIC
# MAGIC Use `mlflow.genai.evaluate` with a custom `ka_guideline_adherence` scorer to
# MAGIC assess whether the KA's responses actually satisfy the expected answer guidelines.

# COMMAND ----------

# DBTITLE 1,Build eval dataset from synthetic questions
from databricks.agents.evals import judges
from mlflow.genai.scorers import scorer
from mlflow.tracing.constant import SpanAttributeKey

# Guidelines are derived from what the KA documents should contain.
# In a real setup these would come from a curated eval_dataset.json in the volume.
EVAL_DATASET = [
    {
        "question": "What are the key brand positioning differences between AutoNova and FreshGlow based on their 2025 brand guidelines?",
        "guidelines": [
            "The answer should compare the brand positioning of AutoNova and FreshGlow directly.",
            "The answer should reference specific elements from their 2025 brand guidelines such as tone, target audience, or value proposition.",
            "The answer should highlight meaningful differences rather than generic brand attributes.",
        ],
    },
    {
        "question": "How did the AutoNova Spring Launch campaign perform against its brief objectives, comparing the campaign brief to the post-campaign report?",
        "guidelines": [
            "The answer should reference both the campaign brief and the post-campaign report.",
            "The answer should compare planned objectives to actual results (e.g. reach, engagement, conversion).",
            "The answer should note where the campaign met, exceeded, or fell short of its goals.",
        ],
    },
    {
        "question": "What media channels and budget allocations are outlined in the AutoNova Q2 2025 media plan, and how do they compare to HealthFirst's Q4 2024 plan?",
        "guidelines": [
            "The answer should list the media channels in the AutoNova Q2 2025 plan with budget figures or percentages.",
            "The answer should reference HealthFirst's Q4 2024 plan for comparison.",
            "The answer should identify notable differences in channel mix or spend allocation between the two plans.",
        ],
    },
    {
        "question": "What were the main findings from the Hilton Value Segmentation research, and how do the qualitative and quantitative reports complement each other?",
        "guidelines": [
            "The answer should summarize the key findings from the Hilton Value Segmentation research.",
            "The answer should distinguish between findings from the qualitative and quantitative reports.",
            "The answer should explain how the two methodologies reinforce or add nuance to each other.",
        ],
    },
    {
        "question": "What consumer trends from the 2026 Global Consumer Predictions report could inform future campaign strategies for brands like QuickBite or StreamPlay?",
        "guidelines": [
            "The answer should reference specific trends from the 2026 Global Consumer Predictions report.",
            "The answer should connect those trends to actionable implications for QuickBite or StreamPlay.",
            "The answer should be forward-looking and strategy-oriented rather than purely descriptive.",
        ],
    },
]

print(f"Eval dataset: {len(EVAL_DATASET)} questions")
for i, row in enumerate(EVAL_DATASET, 1):
    print(f"  {i}. {row['question'][:80]}")

# COMMAND ----------

# DBTITLE 1,Define custom scorer
@scorer
def ka_guideline_adherence(inputs, outputs, trace):
    examples_span = next(
        (span for span in trace.data.spans if span.name == "examples"), None
    )

    EXAMPLES = "examples"
    if examples_span and SpanAttributeKey.OUTPUTS in examples_span.attributes:
        examples_outputs = examples_span.attributes[SpanAttributeKey.OUTPUTS]
        guidelines_context = {EXAMPLES: json.dumps(examples_outputs)}
    else:
        guidelines_context = {EXAMPLES: None}

    implicit_guidelines = [
        ka_instruction,
        *inputs["expectations"]["guidelines"],
        f"The response must satisfy any generalizable guidelines of similar questions in {EXAMPLES}.",
    ]

    return judges.guideline_adherence(
        request=inputs,
        response=outputs,
        guidelines=implicit_guidelines,
        guidelines_context=guidelines_context,
        assessment_name="ka_guideline_adherence",
    )

# COMMAND ----------

# DBTITLE 1,Run evaluation
eval_data = [
    {
        "inputs": {
            "input": [{"role": "user", "content": row["question"]}],
            "expectations": {"guidelines": row["guidelines"]},
        },
    }
    for row in EVAL_DATASET
]

mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=mlflow.genai.to_predict_fn(f"endpoints:/{KA_ENDPOINT}"),
    scorers=[ka_guideline_adherence],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Production Observability — Traces as Delta Tables
# MAGIC
# MAGIC Snapshot your traces to a managed Delta table so you can:
# MAGIC - Query p95 latency by question category with SQL
# MAGIC - JOIN traces with eval results to find systematically wrong answers
# MAGIC - Build Genie spaces or dashboards over trace data

# COMMAND ----------

# DBTITLE 1,Snapshot traces to Delta
import logging as _logging
import warnings as _warnings

_warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
_logging.getLogger("mlflow.tracing.client").setLevel(_logging.ERROR)

TRACES_TABLE_FQN    = f"{CATALOG}.{SCHEMA}.{_short_name}_ka_traces"
TRACES_TABLE_BT_FQN = f"{CATALOG_BT}.{SCHEMA_BT}.{_bt(f'{_short_name}_ka_traces')}"

exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
trace_df = mlflow.search_traces(
    experiment_ids=[exp.experiment_id],
    max_results=500,
    order_by=["timestamp DESC"],
)

def _to_json(v):
    if v is None:
        return None
    return json.dumps(v, default=str) if isinstance(v, (dict, list)) else v

def _to_ms(v):
    if v is None:
        return None
    if hasattr(v, "total_seconds"):
        return int(v.total_seconds() * 1000)
    try:
        return int(v)
    except Exception:
        return None

flat = trace_df.copy()
for col in ("tags", "request", "response", "spans", "request_metadata", "assessments", "info"):
    if col in flat.columns:
        flat[col] = flat[col].apply(_to_json)
if "execution_duration" in flat.columns:
    flat["execution_duration"] = flat["execution_duration"].apply(_to_ms)

(spark.createDataFrame(flat)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACES_TABLE_FQN))

print(f"Persisted {len(flat)} traces to {TRACES_TABLE_FQN}")

# COMMAND ----------

# DBTITLE 1,Latency distribution
display(spark.sql(f"""
  SELECT
    COUNT(*)                                       AS n_traces,
    ROUND(AVG(execution_duration), 1)              AS avg_ms,
    ROUND(PERCENTILE(execution_duration, 0.50), 1) AS p50_ms,
    ROUND(PERCENTILE(execution_duration, 0.95), 1) AS p95_ms,
    MAX(execution_duration)                        AS max_ms
  FROM {TRACES_TABLE_BT_FQN}
  WHERE execution_duration IS NOT NULL
"""))

# COMMAND ----------

# DBTITLE 1,Errors and slow traces
display(spark.sql(f"""
  SELECT
    trace_id,
    state                                        AS status,
    execution_duration                           AS duration_ms,
    get_json_object(tags, '$.mlflow.traceName')  AS trace_name
  FROM {TRACES_TABLE_BT_FQN}
  WHERE state = 'ERROR' OR execution_duration > 5000
  ORDER BY execution_duration DESC
  LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Section | What you did | Key API |
# MAGIC |---------|-------------|---------|
# MAGIC | 1. Generate traces | Sent 5 AdTech questions to the KA | `get_deploy_client().predict()` |
# MAGIC | 2. Search & diagnose | Found slow traces, drilled into spans | `mlflow.search_traces()`, `MlflowClient().get_trace()` |
# MAGIC | 3. Evaluate quality | Scored guideline adherence on 5 Q&A pairs | `mlflow.genai.evaluate()` + custom scorer |
# MAGIC | 4. Observability | Snapshotted traces to Delta for SQL analytics | `search_traces()` → Delta |
# MAGIC
# MAGIC **Next:** Run `02c_identity` to see SP-per-tenant identity passthrough with Genie.
