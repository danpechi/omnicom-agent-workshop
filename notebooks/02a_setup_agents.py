# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # Setup: Omnicom Affinity Hub Agents
# MAGIC
# MAGIC This notebook sets up the two sub-agents that the Supervisor routes between:
# MAGIC
# MAGIC 1. **Knowledge Assistant (KA)** — answers unstructured document questions (methodology, playbooks, account info)
# MAGIC 2. **Genie Space** — already created in `01_setup_data`; this notebook grants tenant SP access
# MAGIC
# MAGIC It also covers the **architecture decision guide** — when to use KA, Genie, or the Supervisor.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Install dependencies and load config
# MAGIC 2. Create the KA via `POST /api/2.1/knowledge-assistants`
# MAGIC 3. Attach the documents volume as a knowledge source
# MAGIC 4. Wait for `state == ACTIVE`
# MAGIC 5. Grant tenant service principals `CAN_QUERY` on the KA endpoint
# MAGIC 6. Test the KA endpoint with an adtech question

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install mlflow[databricks] databricks-sdk --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import time
import mlflow
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Architecture Decision Guide
# MAGIC %md
# MAGIC ## Architecture Decision Guide
# MAGIC
# MAGIC Before building, choose the right tool for each question type:
# MAGIC
# MAGIC | User Question | Right Tool | Why |
# MAGIC |---|---|---|
# MAGIC | "What is an Affinity Loop?" | **Knowledge Assistant** | Unstructured doc Q&A — auto-RAG over uploaded files, no SQL needed |
# MAGIC | "Show my open opportunities" | **Genie Space** | Structured SQL query — NL→SQL, UC-governed, returns live data |
# MAGIC | "Who is this user? Route correctly." | **Supervisor Agent** | Auth, routing, identity passthrough — single API endpoint for the app |
# MAGIC | "AT&T user vs JLR user see different data" | **Supervisor + RLS** | UC `IS_MEMBER()` row filter fires at query time against tenant SP identity |
# MAGIC
# MAGIC ### When NOT to build a Supervisor
# MAGIC
# MAGIC - If all questions are document Q&A → use a single KA directly
# MAGIC - If all questions are structured data → use a Genie Space directly
# MAGIC - Only add a Supervisor when you need **routing + identity passthrough**
# MAGIC
# MAGIC ### Identity Passthrough Pattern
# MAGIC
# MAGIC ```
# MAGIC App (tenant_id="TEN-001" in custom_inputs)
# MAGIC   └── Supervisor Agent
# MAGIC         ├── LLM classifier → "genie"
# MAGIC         ├── Lookup: TEN-001 → SP application_id
# MAGIC         └── Genie API with user_context.user_id = SP application_id
# MAGIC               └── UC RLS fires as tenant SP → AT&T rows only
# MAGIC ```
# MAGIC
# MAGIC The KA is **shared** across all tenants — document content is not tenant-sensitive.
# MAGIC The Genie Space enforces tenant isolation via UC row-level security at query time.

# COMMAND ----------

# DBTITLE 1,Section 1 — Create Knowledge Assistant
# MAGIC %md
# MAGIC ## Section 1: Create the Knowledge Assistant
# MAGIC
# MAGIC One shared KA for all tenants. The documents cover AH methodology, AT&T account info,
# MAGIC campaign playbooks, and case studies — none of which are tenant-sensitive.
# MAGIC
# MAGIC Uses `POST /api/2.1/knowledge-assistants`.

# COMMAND ----------

# DBTITLE 1,Create KA (idempotent)
w = WorkspaceClient()

# display_name must match ^[\w.-]+$ and be 4-63 chars
ka_display_name = KA_NAME[:63]

# Check if already exists first
_existing_kas = w.api_client.do("GET", "/api/2.1/knowledge-assistants").get("knowledge_assistants", [])
_existing_ka = next((k for k in _existing_kas if k.get("display_name") == ka_display_name), None)

if _existing_ka:
    KA_ID = _existing_ka["id"]
    KA_NAME_RESOURCE = _existing_ka.get("name", "")
    KA_ENDPOINT = _existing_ka.get("endpoint_name", KA_ENDPOINT)
    KA_STATE = _existing_ka.get("state", "UNKNOWN")
    print(f"KA already exists — skipping creation.")
else:
    response = w.api_client.do(
        "POST",
        "/api/2.1/knowledge-assistants",
        body={
            "display_name": ka_display_name,
            "description": "Omnicom Affinity Hub Q&A assistant — methodology, AT&T account, campaign playbooks, case studies",
            "instructions": (
                "You are a helpful assistant for Omnicom Affinity Hub account managers. "
                "Answer questions about Affinity Hub methodology, client accounts, campaign playbooks, "
                "and case studies. Cite the source document when possible."
            ),
        },
    )
    KA_ID = response.get("id")
    KA_NAME_RESOURCE = response.get("name", "")
    KA_ENDPOINT = response.get("endpoint_name", KA_ENDPOINT)
    KA_STATE = response.get("state", "CREATING")
    print(f"KA created!")

print(f"  id            : {KA_ID}")
print(f"  name          : {KA_NAME_RESOURCE}")
print(f"  endpoint_name : {KA_ENDPOINT}")
print(f"  state         : {KA_STATE}")

# COMMAND ----------

# DBTITLE 1,Attach knowledge source (documents volume)
# MAGIC %md
# MAGIC ## Attach Knowledge Source
# MAGIC
# MAGIC The shared docs volume contains the five AT&T/Omnicom markdown files written in `01_setup_data`.
# MAGIC Uses `POST /api/2.1/knowledge-assistants/{id}/knowledge-sources`

# COMMAND ----------

# DBTITLE 1,Attach knowledge source (idempotent)
_existing_ks = w.api_client.do(
    "GET", f"/api/2.1/knowledge-assistants/{KA_ID}/knowledge-sources"
).get("knowledge_sources", [])

if _existing_ks:
    KS_ID = _existing_ks[0]["id"]
    print(f"Knowledge source already attached — skipping. (id={KS_ID})")
    print(f"  path  : {DOCS_PATH}")
else:
    ks_response = w.api_client.do(
        "POST",
        f"/api/2.1/knowledge-assistants/{KA_ID}/knowledge-sources",
        body={
            "display_name": "Omnicom Affinity Hub Documents",
            "description": "Unstructured adtech documents — methodology, campaign playbooks, client info, case studies",
            "source_type": "files",
            "files": {"path": UNSTRUCTURED_PATH},
        },
    )
    KS_ID = ks_response.get("id")
    print(f"Knowledge source created!")
    print(f"  id    : {KS_ID}")
    print(f"  state : {ks_response.get('state')}")
    print(f"  path  : {DOCS_PATH}")

# COMMAND ----------

# DBTITLE 1,Section 2 — Wait for KA to be ACTIVE
# MAGIC %md
# MAGIC ## Section 2: Wait for KA Provisioning
# MAGIC
# MAGIC The KA indexes documents and provisions a serving endpoint. Typically 2–10 minutes.
# MAGIC State transitions: `CREATING` → `ACTIVE` (or `FAILED`).

# COMMAND ----------

# DBTITLE 1,Poll until ACTIVE
MAX_WAIT_SECONDS = 1800  # 30 min
POLL_INTERVAL = 30

print(f"Polling KA state (id={KA_ID})...")
start = time.time()
state = "CREATING"

while time.time() - start < MAX_WAIT_SECONDS:
    try:
        ka = w.api_client.do("GET", f"/api/2.1/knowledge-assistants/{KA_ID}")
        state = ka.get("state", "CREATING")
        endpoint_name = ka.get("endpoint_name", "")
        if endpoint_name and endpoint_name != KA_ENDPOINT:
            print(f"  NOTE: endpoint_name updated to '{endpoint_name}'")
            KA_ENDPOINT = endpoint_name
        elapsed = int(time.time() - start)
        print(f"  [{elapsed:>4}s] state={state}  endpoint={KA_ENDPOINT or '(pending)'}")
        if state == "ACTIVE":
            print(f"\nKA is ACTIVE and ready!")
            break
        elif state == "FAILED":
            error = ka.get("error_info", "unknown error")
            raise RuntimeError(f"KA provisioning FAILED: {error}")
    except RuntimeError:
        raise
    except Exception as e:
        print(f"  Poll error (will retry): {e}")
    time.sleep(POLL_INTERVAL)
else:
    print(f"\nWARNING: KA did not reach ACTIVE within {MAX_WAIT_SECONDS // 60} min.")
    print("Re-run the test cell once the KA shows ACTIVE.")

# COMMAND ----------

# DBTITLE 1,Section 3 — Grant Tenant SPs Access to KA Endpoint
# MAGIC %md
# MAGIC ## Section 3: Grant Tenant Service Principals CAN_QUERY on KA Endpoint
# MAGIC
# MAGIC Each tenant SP needs `CAN_QUERY` on the KA serving endpoint so the Supervisor Agent
# MAGIC can call it on behalf of any tenant. Without this grant, the SP would get a 403.

# COMMAND ----------

# DBTITLE 1,Load tenant SPs from table
# Load the SP records we created in 01_setup_data
tenant_sps_df = spark.table(TENANT_SPS_TABLE_BT_FQN)
tenant_sp_records = [row.asDict() for row in tenant_sps_df.collect()]
print(f"Loaded {len(tenant_sp_records)} tenant SP records from {TENANT_SPS_TABLE_FQN}")
for rec in tenant_sp_records:
    print(f"  {rec['tenant_id']}: {rec['display_name']} (app_id={rec['application_id']})")

# COMMAND ----------

# DBTITLE 1,Grant CAN_QUERY on KA endpoint to each tenant SP
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

try:
    endpoint_obj = w.serving_endpoints.get(KA_ENDPOINT)
    endpoint_id = endpoint_obj.id

    acl = [
        ServingEndpointAccessControlRequest(
            service_principal_name=rec["application_id"],
            permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
        )
        for rec in tenant_sp_records
    ]

    w.serving_endpoints.update_permissions(endpoint_id, access_control_list=acl)
    print(f"Granted CAN_QUERY on '{KA_ENDPOINT}' to {len(acl)} tenant SPs.")
except Exception as e:
    print(f"Permission grant skipped (non-fatal): {e}")
    print("Tenant SPs may not be able to call the KA endpoint directly.")

# COMMAND ----------

# DBTITLE 1,Section 4 — Test KA Endpoint
# MAGIC %md
# MAGIC ## Section 4: Test the KA Endpoint
# MAGIC
# MAGIC Verify the KA is responding to adtech questions.

# COMMAND ----------

# DBTITLE 1,Test KA endpoint
from mlflow.deployments import get_deploy_client


def test_ka_endpoint(endpoint_name: str, question: str) -> str:
    client = get_deploy_client("databricks")
    response = client.predict(
        endpoint=endpoint_name,
        inputs={"input": [{"role": "user", "content": question}]},
    )
    # KA returns Responses API format: output[].content[].text
    for item in response.get("output", []):
        for part in item.get("content", []):
            if part.get("type") == "output_text":
                return part["text"]
    return str(response)


TEST_QUESTION = "What is an Affinity Loop and what are the different types?"

try:
    print(f"Testing endpoint: {KA_ENDPOINT}")
    answer = test_ka_endpoint(KA_ENDPOINT, TEST_QUESTION)
    print("=" * 60)
    print(answer)
    print("=" * 60)
    print("\nKA is responding. Proceed to 02b_tracing_deep_dive.")
except Exception as e:
    print(f"Could not reach '{KA_ENDPOINT}': {e}")
    print("KA may still be indexing. Check Agents UI for state.")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Status |
# MAGIC |------|--------|
# MAGIC | Documents uploaded to UC Volume | Done in `01_setup_data` |
# MAGIC | Genie Space created | Done in `01_setup_data` |
# MAGIC | Service principal created | Done in `01_setup_data` |
# MAGIC | KA created via API | Done |
# MAGIC | Knowledge source (docs volume) attached | Done |
# MAGIC | KA reached ACTIVE state | Done |
# MAGIC | Tenant SPs granted CAN_QUERY on KA | Done |
# MAGIC | KA endpoint tested | Done |
# MAGIC
# MAGIC **Note your KA ID above** — you will need it in `02b_setup_supervisor`.
# MAGIC
# MAGIC **Next:** Run `02b_setup_supervisor` to create the Supervisor Agent, then
# MAGIC `02c_tracing_deep_dive` to explore traces and evaluation.
