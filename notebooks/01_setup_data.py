# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Data & Identity Resources
# MAGIC
# MAGIC Creates the three resources needed before running the agents:
# MAGIC 1. **Genie Space** — points to the shared structured tables in `databricks_workshop.sample`
# MAGIC 2. **UC Groups** — one group per tenant + admin group
# MAGIC 3. **Tenant Service Principals** — one SP per tenant, added to its group, saved to a lookup table

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Genie Space
# MAGIC
# MAGIC Points to the five shared tables in `databricks_workshop.sample`.
# MAGIC Each workshop participant gets their own Genie Space (user-scoped name).

# COMMAND ----------

# Find a running SQL warehouse
warehouses = sorted(
    list(w.warehouses.list()),
    key=lambda wh: 0 if (wh.state and wh.state.name == "RUNNING") else 1,
)
if not warehouses:
    raise RuntimeError("No SQL warehouses found. Create a warehouse and retry.")

warehouse_id = warehouses[0].id
print(f"Using warehouse: {warehouses[0].name} ({warehouse_id})")

# Check if Genie Space already exists, create if not.
# Genie spaces are managed via the /api/2.0/data-rooms/ endpoint.
_spaces_resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
existing_spaces = _spaces_resp.get("genie_spaces", []) if isinstance(_spaces_resp, dict) else []
existing = next((s for s in existing_spaces if s.get("title") == GENIE_NAME), None)

if existing:
    GENIE_SPACE_ID = existing["space_id"]
    print(f"Genie Space already exists: {GENIE_NAME} (id={GENIE_SPACE_ID})")
else:
    space = w.api_client.do(
        "POST",
        "/api/2.0/data-rooms/",
        body={
            "display_name": GENIE_NAME,
            "description": f"Omnicom Affinity Hub — AdTech structured data for {_short_name}",
            "warehouse_id": warehouse_id,
            "table_identifiers": list(DATA_TABLES.values()),
        },
    )
    GENIE_SPACE_ID = space.get("space_id") or space.get("id")
    print(f"Created Genie Space: {GENIE_NAME} (id={GENIE_SPACE_ID})")

print(f"\nGenie Space ID: {GENIE_SPACE_ID}")
print(f"Tables added:")
for fqn in DATA_TABLES.values():
    print(f"  - {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Service Principal
# MAGIC
# MAGIC One SP with access to all data. Its `application_id` is saved to a lookup table
# MAGIC used by the Supervisor Agent for identity passthrough to Genie.

# COMMAND ----------

sp_display_name = f"{_short_name}-adtech-sp"

try:
    sp = w.service_principals.create(display_name=sp_display_name)
    print(f"Created SP: {sp_display_name} (id={sp.id})")
except Exception as e:
    if "already exists" in str(e).lower() or "conflict" in str(e).lower():
        sp = next(
            (s for s in w.service_principals.list(filter=f'displayName eq "{sp_display_name}"')
             if s.display_name == sp_display_name),
            None,
        )
        if sp:
            print(f"Exists:  {sp_display_name} (id={sp.id})")
        else:
            raise RuntimeError(f"Could not create or find SP '{sp_display_name}': {e}")
    else:
        raise

tenant_sp_records = [{
    "tenant_id": _short_name,
    "sp_id": str(sp.id),
    "application_id": str(sp.application_id),
    "display_name": sp_display_name,
    "group_name": "",
}]

print(f"\nSP ready: application_id={sp.application_id}")

# Save lookup table for the Supervisor Agent
sp_df = spark.createDataFrame(tenant_sp_records)
sp_df.write.mode("overwrite").saveAsTable(TENANT_SPS_TABLE_BT_FQN)
print(f"Saved: {TENANT_SPS_TABLE_FQN}")
sp_df.show(truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC | Resource | Value |
# MAGIC |---|---|
# MAGIC | Genie Space | `{GENIE_NAME}` (id printed above) |
# MAGIC | Service Principal | `{sp_display_name}` — full access to all client data |
# MAGIC | SP lookup table | `{TENANT_SPS_TABLE_FQN}` |
# MAGIC
# MAGIC **Next:** Run `02a_setup_agents` to create the Knowledge Assistant.

# COMMAND ----------

print("=" * 55)
print("  SETUP COMPLETE")
print("=" * 55)
print(f"  Genie Space : {GENIE_NAME} (id={GENIE_SPACE_ID})")
print(f"  Groups      : {len(TENANT_GROUPS) + 1} created")
print(f"  Tenant SPs  : {len(tenant_sp_records)} created → {TENANT_SPS_TABLE_FQN}")
print("=" * 55)
