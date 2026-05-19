# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # Setup: Supervisor Agent
# MAGIC
# MAGIC This notebook creates the **Supervisor Agent** that sits on top of your Knowledge Assistant
# MAGIC and Genie Space, routing questions to the right tool automatically.
# MAGIC
# MAGIC **Before running this notebook:**
# MAGIC 1. `01_setup_data` — Genie Space must exist (you need its Space ID)
# MAGIC 2. `02a_setup_agents` — KA must be ACTIVE (you need its tile ID)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Accept KA tile ID and Genie Space ID as manual inputs
# MAGIC 2. Create the Supervisor Agent (idempotent)
# MAGIC 3. Attach the KA and Genie Space as tools
# MAGIC 4. Test the Supervisor Agent endpoint

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install mlflow[databricks] databricks-sdk openai --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Enter your IDs
# MAGIC
# MAGIC Fill in the two widgets below before running the rest of the notebook.
# MAGIC
# MAGIC - **KA Tile ID** — from the output of `02a_setup_agents`, cell "Create KA (idempotent)". Looks like a UUID: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
# MAGIC - **Genie Space ID** — from the output of `01_setup_data`, or find it in the Databricks UI under **Genie** → your space → the URL contains the ID. Looks like: `01xxxxxxxxxxxxxxxxxx`

# COMMAND ----------

# DBTITLE 1,Input widgets
dbutils.widgets.text("ka_tile_id",     "", "KA Tile ID (from 02a output)")
dbutils.widgets.text("genie_space_id", "", "Genie Space ID (from 01 output or Genie UI)")

# COMMAND ----------

# DBTITLE 1,Read and validate inputs
KA_TILE_ID     = dbutils.widgets.get("ka_tile_id").strip()
GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id").strip()

if not KA_TILE_ID:
    raise ValueError("KA Tile ID is required. Copy it from the 02a_setup_agents output.")
if not GENIE_SPACE_ID:
    raise ValueError("Genie Space ID is required. Copy it from the 01_setup_data output or the Genie UI URL.")

print(f"KA Tile ID     : {KA_TILE_ID}")
print(f"Genie Space ID : {GENIE_SPACE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Supervisor Agent

# COMMAND ----------

# DBTITLE 1,Create Supervisor Agent (idempotent)
w = WorkspaceClient()

supervisor_name = f"{_short_name}-adtech-supervisor"

_existing_sas = w.api_client.do("GET", "/api/2.1/supervisor-agents").get("supervisor_agents", [])
_existing_sa = next((s for s in _existing_sas if s.get("display_name") == supervisor_name), None)

if _existing_sa:
    SUPERVISOR_RESOURCE_NAME = _existing_sa["name"]
    SUPERVISOR_ENDPOINT_NAME = _existing_sa.get("endpoint_name", "")
    print(f"Supervisor Agent already exists — skipping creation.")
else:
    _sa = w.api_client.do(
        "POST",
        "/api/2.1/supervisor-agents",
        body={
            "display_name": supervisor_name,
            "description": (
                "Omnicom Affinity Hub Supervisor Agent. Routes AdTech questions to the "
                "Knowledge Assistant (brand docs, campaign briefs, research reports) or "
                "Genie Space (campaign metrics, opportunities, structured data)."
            ),
        },
    )
    SUPERVISOR_RESOURCE_NAME = _sa["name"]
    SUPERVISOR_ENDPOINT_NAME = _sa.get("endpoint_name", "")
    print(f"Created Supervisor Agent: {supervisor_name}")

print(f"  resource name  : {SUPERVISOR_RESOURCE_NAME}")
print(f"  endpoint name  : {SUPERVISOR_ENDPOINT_NAME or '(provisioning...)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Attach Tools

# COMMAND ----------

# DBTITLE 1,Attach KA and Genie Space as tools (idempotent)
_existing_tools = w.api_client.do(
    "GET", f"/api/2.1/{SUPERVISOR_RESOURCE_NAME}/tools"
).get("tools", [])
_existing_tool_ids = {t.get("tool_id") for t in _existing_tools}

# Knowledge Assistant tool
if "ka_tool" not in _existing_tool_ids:
    w.api_client.do(
        "POST",
        f"/api/2.1/{SUPERVISOR_RESOURCE_NAME}/tools",
        query={"tool_id": "ka_tool"},
        body={
            "tool_type": "knowledge_assistant",
            "description": (
                "Answers questions about brand guidelines, campaign briefs, media plans, "
                "research reports, and strategic documents."
            ),
            "knowledge_assistant": {
                "knowledge_assistant_id": KA_TILE_ID,
            },
        },
    )
    print("+ Added Knowledge Assistant tool")
else:
    print("~ Knowledge Assistant tool already exists")

# Genie Space tool
if "genie_tool" not in _existing_tool_ids:
    w.api_client.do(
        "POST",
        f"/api/2.1/{SUPERVISOR_RESOURCE_NAME}/tools",
        query={"tool_id": "genie_tool"},
        body={
            "tool_type": "genie_space",
            "description": (
                "Answers questions about campaign performance, opportunities, client data, "
                "and any question that requires querying structured data."
            ),
            "genie_space": {
                "id": GENIE_SPACE_ID,
            },
        },
    )
    print("+ Added Genie Space tool")
else:
    print("~ Genie Space tool already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Wait for Endpoint
# MAGIC
# MAGIC The Supervisor Agent provisions a serving endpoint (prefix `mas-`). This typically takes 2–5 minutes.

# COMMAND ----------

# DBTITLE 1,Poll until endpoint is ready
import time

MAX_WAIT_SECONDS = 600
POLL_INTERVAL = 20

print(f"Polling Supervisor Agent endpoint...")
start = time.time()

while time.time() - start < MAX_WAIT_SECONDS:
    _sa_status = w.api_client.do("GET", f"/api/2.1/{SUPERVISOR_RESOURCE_NAME}")
    endpoint_name = _sa_status.get("endpoint_name", "")
    state = _sa_status.get("state", "CREATING")
    elapsed = int(time.time() - start)
    print(f"  [{elapsed:>3}s] state={state}  endpoint={endpoint_name or '(pending)'}")
    if endpoint_name and state == "ACTIVE":
        SUPERVISOR_ENDPOINT_NAME = endpoint_name
        print(f"\nSupervisor Agent is ACTIVE!")
        break
    time.sleep(POLL_INTERVAL)
else:
    print(f"\nWARNING: Did not reach ACTIVE within {MAX_WAIT_SECONDS // 60} min.")
    print("Check the Agents UI and re-run the test cell once it's ready.")

print(f"\nSupervisor endpoint name: {SUPERVISOR_ENDPOINT_NAME}")
print("Use this name in the demo app's 'Supervisor Agent' field.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test the Supervisor Agent

# COMMAND ----------

# DBTITLE 1,Test — doc question (should route to KA)
import os
from openai import OpenAI

DATABRICKS_HOST  = (os.getenv("DATABRICKS_HOST") or dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()).rstrip("/")
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

_client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_HOST}/serving-endpoints",
)

_response = _client.responses.create(
    model=SUPERVISOR_ENDPOINT_NAME,
    input=[{"role": "user", "content": "What are the key brand positioning differences between AutoNova and FreshGlow?"}],
)

print("Doc question → KA routing:")
print("=" * 60)
print(" ".join(
    getattr(c, "text", "")
    for o in _response.output
    for c in getattr(o, "content", [])
))
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Test — data question (should route to Genie)
_response2 = _client.responses.create(
    model=SUPERVISOR_ENDPOINT_NAME,
    input=[{"role": "user", "content": "How many opportunities are there grouped by tenant name?"}],
)

print("Data question → Genie routing:")
print("=" * 60)
print(" ".join(
    getattr(c, "text", "")
    for o in _response2.output
    for c in getattr(o, "content", [])
))
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Output |
# MAGIC |------|--------|
# MAGIC | Supervisor Agent created | `{supervisor_name}` |
# MAGIC | KA tool attached | tile ID: `{KA_TILE_ID}` |
# MAGIC | Genie Space tool attached | space ID: `{GENIE_SPACE_ID}` |
# MAGIC | Serving endpoint | paste into demo app |
# MAGIC
# MAGIC **Copy the endpoint name** printed above (e.g. `mas-xxxxxxxx-endpoint`) and paste it
# MAGIC into the **Supervisor Agent** field in the demo app.
# MAGIC
# MAGIC **Next:** Run `02c_tracing_deep_dive` to explore MLflow traces and run evaluation.
