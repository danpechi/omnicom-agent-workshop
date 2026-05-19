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

# Check if Genie Space already exists
existing_spaces = list(w.genie.list_spaces())
existing = next((s for s in existing_spaces if s.title == GENIE_NAME), None)

if existing:
    GENIE_SPACE_ID = existing.space_id
    print(f"Genie Space already exists: {GENIE_NAME} (id={GENIE_SPACE_ID})")
else:
    space = w.genie.create_space(
        title=GENIE_NAME,
        description=f"Omnicom Affinity Hub — AdTech structured data for {_short_name}",
        warehouse_id=warehouse_id,
        table_identifiers=list(DATA_TABLES.values()),
    )
    GENIE_SPACE_ID = space.space_id
    print(f"Created Genie Space: {GENIE_NAME} (id={GENIE_SPACE_ID})")

print(f"\nGenie Space ID: {GENIE_SPACE_ID}")
print(f"Tables added:")
for fqn in DATA_TABLES.values():
    print(f"  - {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create UC Groups
# MAGIC
# MAGIC One group per tenant + one admin group. Row-level security uses `IS_MEMBER()` against these.

# COMMAND ----------

for group_name in [*TENANT_GROUPS.values(), ADMIN_GROUP]:
    try:
        w.groups.create(display_name=group_name)
        print(f"Created: {group_name}")
    except Exception as e:
        if "already exists" in str(e).lower() or "conflict" in str(e).lower():
            print(f"Exists:  {group_name}")
        else:
            print(f"WARN:    {group_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Tenant Service Principals
# MAGIC
# MAGIC One Databricks SP per tenant. Each SP is added to its tenant group and its `application_id`
# MAGIC is saved to a lookup table used by the Supervisor Agent for identity passthrough to Genie.

# COMMAND ----------

tenant_sp_records = []

for tenant_id, group_name in TENANT_GROUPS.items():
    sp_display_name = f"{_short_name}-{tenant_id.lower().replace('-', '')}-sp"

    # Create or find the SP
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
                print(f"WARN: Could not create or find {sp_display_name}: {e}")
                continue
        else:
            print(f"WARN: {sp_display_name}: {e}")
            continue

    # Add SP to its tenant group
    try:
        group_list = list(w.groups.list(filter=f'displayName eq "{group_name}"'))
        group = next((g for g in group_list if g.display_name == group_name), None)
        if group:
            w.groups.patch(
                id=group.id,
                operations=[{"op": "add", "path": "members", "value": [{"value": str(sp.id)}]}],
            )
            print(f"  → added to {group_name}")
    except Exception as e:
        print(f"  WARN: Could not add to group: {e}")

    tenant_sp_records.append({
        "tenant_id": tenant_id,
        "sp_id": str(sp.id),
        "application_id": str(sp.application_id),
        "display_name": sp_display_name,
        "group_name": group_name,
    })

print(f"\n{len(tenant_sp_records)} tenant SPs ready.")

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
# MAGIC | UC Groups | 5 tenant groups + 1 admin group |
# MAGIC | Tenant SPs | 5 SPs saved to `{TENANT_SPS_TABLE_FQN}` |
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
