# Databricks notebook source
# MAGIC %md
# MAGIC # Identity Passthrough — SP-per-Tenant Pattern
# MAGIC
# MAGIC This notebook covers how the Omnicom Affinity Hub handles **multi-tenant identity**:
# MAGIC who is calling the Supervisor Agent, and how that identity flows through to Genie so
# MAGIC Unity Catalog row-level security fires correctly.
# MAGIC
# MAGIC **What this notebook covers:**
# MAGIC 1. How the frontend app connects to the Supervisor API
# MAGIC 2. Three identity patterns — trade-offs and when to use each
# MAGIC 3. The SP-per-tenant implementation we built
# MAGIC 4. Live demo: same question, different tenant_ids → different row counts
# MAGIC 5. Genie Conversational API deep dive

# COMMAND ----------

# MAGIC %pip install mlflow[databricks] databricks-sdk requests --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time

import requests
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: How the Frontend Connects to the Supervisor API
# MAGIC
# MAGIC The Supervisor Agent is deployed as a **Databricks Model Serving endpoint**.
# MAGIC The frontend (a Databricks App) calls it like any other REST endpoint.
# MAGIC
# MAGIC ### Request Format (Responses API)
# MAGIC
# MAGIC ```python
# MAGIC POST /serving-endpoints/{supervisor_endpoint}/invocations
# MAGIC Authorization: Bearer <token>
# MAGIC Content-Type: application/json
# MAGIC
# MAGIC {
# MAGIC   "input": [
# MAGIC     {"role": "user", "content": "Show my open opportunities"}
# MAGIC   ],
# MAGIC   "custom_inputs": {
# MAGIC     "tenant_id": "TEN-001"   # AT&T
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC The `tenant_id` in `custom_inputs` tells the Supervisor which tenant is asking.
# MAGIC The Supervisor maps this to a service principal, then passes it as `user_context`
# MAGIC to Genie so UC row-level security fires against that SP's group membership.
# MAGIC
# MAGIC ### Auth Options
# MAGIC
# MAGIC | Auth Method | When to Use |
# MAGIC |---|---|
# MAGIC | PAT (Personal Access Token) | Dev/testing only — not for production |
# MAGIC | OAuth M2M | App-to-app calls from Databricks Apps, CI/CD |
# MAGIC | OAuth U2M | When the frontend user's identity matters end-to-end |
# MAGIC
# MAGIC For the workshop, we use the `DATABRICKS_TOKEN` environment variable (PAT or OAuth token
# MAGIC injected automatically by Databricks Apps via identity passthrough).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Three Identity Patterns
# MAGIC
# MAGIC When building multi-tenant agents on Databricks, there are three common patterns.
# MAGIC Understand the trade-offs before choosing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 1 — Single Service Principal (App SP)
# MAGIC
# MAGIC ```
# MAGIC Frontend → Supervisor (runs as App SP) → Genie (runs as App SP) → UC sees App SP
# MAGIC ```
# MAGIC
# MAGIC **How it works:** All calls to Genie run as the app's own SP. UC RLS can filter by
# MAGIC a tenant column, but only if the SQL contains an explicit `WHERE tenant_id = ?`.
# MAGIC
# MAGIC **Best for:** Internal tools where all users trust the same access level, or when
# MAGIC you want the app to manage filtering explicitly in SQL.
# MAGIC
# MAGIC **Limitation:** One SP = one view of all data. Can't use `IS_MEMBER()` RLS because
# MAGIC the app SP isn't a member of any tenant group. Governance is "soft" (app-enforced).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 2 — user_id Tag (Simulated Identity)
# MAGIC
# MAGIC ```
# MAGIC Frontend → Supervisor (App SP) → Genie with user_id tag → RLS uses lookup table
# MAGIC ```
# MAGIC
# MAGIC **How it works:** App SP calls everything, passes `user_id` as metadata.
# MAGIC RLS filter checks a lookup table (not `IS_MEMBER()`).
# MAGIC
# MAGIC **Best for:** Audit trails and soft multi-tenancy without real SP provisioning.
# MAGIC
# MAGIC **Limitation:** The lookup table is app-managed — a bug could expose wrong data.
# MAGIC Not enforceable by UC independent of the app.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 3 — SP-per-Tenant (What We Built)
# MAGIC
# MAGIC ```
# MAGIC Frontend (tenant_id="TEN-001")
# MAGIC   └── Supervisor Agent
# MAGIC         ├── tenant_id → SP lookup → AT&T SP (application_id)
# MAGIC         └── Genie API with user_context.user_id = AT&T SP application_id
# MAGIC               └── UC evaluates IS_MEMBER() as AT&T SP → only AT&T rows
# MAGIC ```
# MAGIC
# MAGIC **How it works:** One Databricks SP per tenant. Each SP is a member of its tenant group.
# MAGIC When the Supervisor calls Genie, it passes the tenant SP's `application_id` as
# MAGIC `user_context.user_id`. Genie executes the SQL query as that SP identity — so UC's
# MAGIC `IS_MEMBER()` row filter evaluates correctly.
# MAGIC
# MAGIC **Best for:** Real multi-tenant enforcement where UC is the source of truth.
# MAGIC Governance is hard (enforced by UC, not app code).
# MAGIC
# MAGIC **Trade-off:** Requires creating and managing one SP per tenant. Genie must support
# MAGIC `user_context` (it does as of 2024).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: SP-per-Tenant Implementation

# COMMAND ----------

# Load the tenant SP map (same table used by agent.py at runtime)
tenant_sps_df = spark.table(TENANT_SPS_TABLE_BT_FQN)
tenant_sp_records = {row["tenant_id"]: row.asDict() for row in tenant_sps_df.collect()}

print("Tenant SP lookup table:")
print(f"{'Tenant ID':<12} {'Application ID':<40} {'Display Name'}")
print("-" * 80)
for tid, rec in tenant_sp_records.items():
    print(f"{tid:<12} {rec['application_id']:<40} {rec['display_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How the Supervisor resolves identity
# MAGIC
# MAGIC When a request arrives with `custom_inputs.tenant_id = "TEN-001"`:
# MAGIC
# MAGIC ```python
# MAGIC # 1. Extract tenant_id from custom_inputs
# MAGIC tenant_id = request.custom_inputs.get("tenant_id")   # "TEN-001"
# MAGIC
# MAGIC # 2. Look up the SP application_id
# MAGIC tenant_sp_app_id = _TENANT_SP_MAP.get(tenant_id)     # e.g. "123456789"
# MAGIC
# MAGIC # 3. Call Genie with user_context
# MAGIC POST /api/2.0/genie/spaces/{space_id}/start-conversation
# MAGIC {
# MAGIC   "content": "Show my open opportunities",
# MAGIC   "user_context": {
# MAGIC     "user_id": "123456789"   # AT&T SP application_id
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # 4. UC evaluates the row filter as the AT&T SP:
# MAGIC #    IS_MEMBER('{short_name}-att-users') → True → AT&T rows only
# MAGIC ```
# MAGIC
# MAGIC The key insight: **Genie executes the SQL as the tenant SP**, not as your app SP.
# MAGIC Unity Catalog enforces `IS_MEMBER()` against the SP's group membership at query time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Live Demo — Same Question, Different Tenants
# MAGIC
# MAGIC We'll call the Genie Conversational API directly (bypassing the Supervisor) to
# MAGIC show what each tenant SP sees. The question is the same; the identity differs.

# COMMAND ----------

# Helper: call Genie with optional user_context
w = WorkspaceClient()
DATABRICKS_HOST_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
if not DATABRICKS_HOST_URL.startswith("http"):
    DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST_URL}"

# Get a token for API calls (uses the current user's credentials in the notebook)
DATABRICKS_TOKEN_VAL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def call_genie_as(question: str, space_id: str, tenant_sp_app_id: str = "", label: str = "") -> str:
    """Call the Genie API with optional tenant SP identity passthrough."""
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN_VAL}",
        "Content-Type": "application/json",
    }
    body = {"content": question}
    if tenant_sp_app_id:
        body["user_context"] = {"user_id": tenant_sp_app_id}

    try:
        start_resp = requests.post(
            f"{DATABRICKS_HOST_URL}/api/2.0/genie/spaces/{space_id}/start-conversation",
            headers=headers, json=body, timeout=30,
        )
        start_resp.raise_for_status()
        data = start_resp.json()
        conv_id = data["conversation_id"]
        msg_id = data["message_id"]
    except Exception as e:
        return f"[{label}] Failed to start conversation: {e}"

    # Poll for result
    for _ in range(60):
        time.sleep(1.0)
        poll = requests.get(
            f"{DATABRICKS_HOST_URL}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers, timeout=15,
        )
        msg = poll.json()
        status = msg.get("status", "")
        if status == "COMPLETED":
            for att in msg.get("attachments", []):
                q = att.get("query", {})
                if q.get("description"):
                    return q["description"]
            return msg.get("content", "No result.")
        elif status in ("FAILED", "CANCELLED"):
            return f"Genie failed: {msg.get('error', 'unknown')}"
    return "Timed out."


# COMMAND ----------

# Resolve Genie Space ID
spaces = list(w.genie.list_spaces())
genie_space = next((s for s in spaces if s.title == GENIE_NAME), None)

if genie_space is None:
    print(f"WARNING: Genie Space '{GENIE_NAME}' not found.")
    print("Make sure 01_setup_data completed successfully.")
    GENIE_SPACE_ID_LIVE = ""
else:
    GENIE_SPACE_ID_LIVE = genie_space.space_id
    print(f"Genie Space found: {GENIE_NAME} (id={GENIE_SPACE_ID_LIVE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: Count open opportunities by tenant
# MAGIC
# MAGIC We ask the same question three times:
# MAGIC - As **AT&T SP** → should see only AT&T opportunities
# MAGIC - As **JLR SP** → should see only JLR opportunities
# MAGIC - As **current user (admin)** → should see all opportunities

# COMMAND ----------

DEMO_QUESTION = "How many open opportunities are there, grouped by tenant name?"

if GENIE_SPACE_ID_LIVE:
    demos = [
        ("TEN-001 (AT&T SP)",  tenant_sp_records.get("TEN-001", {}).get("application_id", "")),
        ("TEN-002 (JLR SP)",   tenant_sp_records.get("TEN-002", {}).get("application_id", "")),
        ("Admin (no SP)",      ""),   # runs as current notebook user
    ]

    for label, sp_app_id in demos:
        print(f"\n{'='*60}")
        print(f"  Identity: {label}")
        print(f"  SP app_id: {sp_app_id or '(current user)'}")
        print("=" * 60)
        result = call_genie_as(DEMO_QUESTION, GENIE_SPACE_ID_LIVE, sp_app_id, label)
        print(result)
else:
    print("Skipping demo — Genie Space not configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC | Identity | Expected rows | Why |
# MAGIC |---|---|---|
# MAGIC | AT&T SP | AT&T only (~5) | SP is member of `{short_name}-att-users` → `IS_MEMBER()` = True only for TEN-001 |
# MAGIC | JLR SP | JLR only (~3) | SP is member of `{short_name}-jlr-users` → `IS_MEMBER()` = True only for TEN-002 |
# MAGIC | Admin (you) | All 15 | You are account admin or member of `{short_name}-omnicom-admin` |
# MAGIC
# MAGIC The UC row filter function enforces this — not the app code. Even if the Supervisor
# MAGIC had a bug and passed the wrong `user_context`, UC would still return the correct rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Genie Conversational API Deep Dive
# MAGIC
# MAGIC Understanding the full API flow helps you debug and extend the Supervisor.
# MAGIC
# MAGIC ### Step 1: Start a conversation
# MAGIC
# MAGIC ```
# MAGIC POST /api/2.0/genie/spaces/{space_id}/start-conversation
# MAGIC {
# MAGIC   "content": "Show me AT&T opportunities with high impact score",
# MAGIC   "user_context": {"user_id": "123456789"}   # optional SP application_id
# MAGIC }
# MAGIC Response: {"conversation_id": "...", "message_id": "..."}
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Poll for completion
# MAGIC
# MAGIC ```
# MAGIC GET /api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}
# MAGIC Response: {
# MAGIC   "status": "COMPLETED",   # or PENDING, EXECUTING_QUERY, FAILED
# MAGIC   "attachments": [
# MAGIC     {
# MAGIC       "query": {
# MAGIC         "description": "There are 5 AT&T opportunities...",   # ← natural language summary
# MAGIC         "query": "SELECT * FROM opportunities WHERE ...",      # ← the generated SQL
# MAGIC         "result": {...}
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Key Fields
# MAGIC
# MAGIC | Field | What it is |
# MAGIC |---|---|
# MAGIC | `status` | `PENDING` → `FILTERING_CONTEXT` → `EXECUTING_QUERY` → `COMPLETED` |
# MAGIC | `attachments[].query.description` | Natural-language summary — what the Supervisor returns |
# MAGIC | `attachments[].query.query` | The SQL Genie generated — useful for debugging |
# MAGIC | `user_context.user_id` | SP `application_id` — Genie executes SQL as this identity |
# MAGIC
# MAGIC ### Why `application_id` not `id`?
# MAGIC
# MAGIC - `sp.id` — internal Databricks SP object ID (used in group membership)
# MAGIC - `sp.application_id` — the OAuth client ID / UC identity string (used in `user_context`)
# MAGIC
# MAGIC Always pass `application_id` to `user_context.user_id`, not the internal `id`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Pattern | Governance | Complexity | When to use |
# MAGIC |---|---|---|---|
# MAGIC | Single App SP | Soft (app-enforced) | Low | Internal tools, trusted users |
# MAGIC | user_id tag | Soft (lookup table) | Medium | Audit trails, no SP provisioning |
# MAGIC | SP-per-tenant | Hard (UC-enforced) | High | Real multi-tenant, UC as source of truth |
# MAGIC
# MAGIC We built Pattern 3. The Supervisor maps `tenant_id` → SP `application_id` → `user_context`
# MAGIC on every Genie call. UC evaluates `IS_MEMBER()` at query time against the SP's identity.
# MAGIC
# MAGIC **Next:** Run `02d_governance` to verify UC grants, test RLS directly, and explore audit logs.
