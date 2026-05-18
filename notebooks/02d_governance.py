# Databricks notebook source
# MAGIC %md
# MAGIC # Governance — UC Grants, RLS, Column Masking, and Audit Logs
# MAGIC
# MAGIC This notebook demonstrates Unity Catalog governance in action for the
# MAGIC Omnicom Affinity Hub multi-tenant data model.
# MAGIC
# MAGIC **What this notebook covers:**
# MAGIC 1. Grant table access to each tenant group
# MAGIC 2. Verify row-level security — confirm each tenant SP sees only its rows
# MAGIC 3. Verify column masking — confirm `budget` is NULL for non-admins
# MAGIC 4. Grant tenant SPs access to the Genie Space
# MAGIC 5. UC audit log queries via `system.access.audit`

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

w = WorkspaceClient()

# Load tenant SP records
tenant_sps_df = spark.table(TENANT_SPS_TABLE_BT_FQN)
tenant_sp_records = [row.asDict() for row in tenant_sps_df.collect()]
print(f"Loaded {len(tenant_sp_records)} tenant SP records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Grant Table Access to Tenant Groups
# MAGIC
# MAGIC We grant `SELECT` on both tables to each tenant group. The row-level security filter
# MAGIC ensures each tenant only sees its own rows — the `GRANT` is what allows the query
# MAGIC to run at all; RLS is what scopes the results.
# MAGIC
# MAGIC **Without a GRANT:** The SP gets a permission denied error (can't query the table).
# MAGIC **Without RLS:** The SP can see all rows (governance hole).
# MAGIC Both are required for correct multi-tenant data access.

# COMMAND ----------

# Grant SELECT on opportunities and campaigns to each tenant group
for tenant_id, group_name in TENANT_GROUPS.items():
    try:
        spark.sql(f"GRANT SELECT ON TABLE {OPPORTUNITIES_TABLE_BT_FQN} TO `{group_name}`")
        spark.sql(f"GRANT SELECT ON TABLE {CAMPAIGNS_TABLE_BT_FQN} TO `{group_name}`")
        print(f"  Granted SELECT to {group_name}")
    except Exception as e:
        print(f"  WARN ({group_name}): {e}")

# Admin group gets SELECT too (RLS lets them see all rows)
try:
    spark.sql(f"GRANT SELECT ON TABLE {OPPORTUNITIES_TABLE_BT_FQN} TO `{ADMIN_GROUP}`")
    spark.sql(f"GRANT SELECT ON TABLE {CAMPAIGNS_TABLE_BT_FQN} TO `{ADMIN_GROUP}`")
    print(f"  Granted SELECT to {ADMIN_GROUP}")
except Exception as e:
    print(f"  WARN ({ADMIN_GROUP}): {e}")

# Also grant USE CATALOG and USE SCHEMA so groups can resolve the FQN
try:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG_BT} TO `{ADMIN_GROUP}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG_BT}.{SCHEMA_BT} TO `{ADMIN_GROUP}`")
    for group_name in TENANT_GROUPS.values():
        spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG_BT} TO `{group_name}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG_BT}.{SCHEMA_BT} TO `{group_name}`")
    print("Granted USE CATALOG + USE SCHEMA to all groups.")
except Exception as e:
    print(f"  WARN (catalog/schema grants): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Verify Row-Level Security
# MAGIC
# MAGIC Running as the current notebook user (who should be account admin or in the admin group),
# MAGIC you can see all rows. We also simulate what each tenant SP would see by calling the
# MAGIC RLS filter function directly with a known tenant_id.

# COMMAND ----------

# Admin view: all rows
print("Admin view (all rows):")
spark.sql(f"SELECT tenant_name, COUNT(*) AS n FROM {OPPORTUNITIES_TABLE_BT_FQN} GROUP BY 1 ORDER BY 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate tenant views by calling the filter function directly
# MAGIC
# MAGIC The row filter is a UC function: `tenant_row_filter(tenant_id STRING) RETURN BOOLEAN`.
# MAGIC We can test it by calling it directly in SQL. In production, UC calls it automatically
# MAGIC when the tenant SP queries the table.

# COMMAND ----------

for tenant_id, group_name in TENANT_GROUPS.items():
    print(f"\n{tenant_id} ({group_name}) would see:")
    try:
        # Call the filter function for this tenant_id — returns True/False
        result = spark.sql(f"""
            SELECT tenant_name, COUNT(*) AS n
            FROM {OPPORTUNITIES_TABLE_BT_FQN}
            WHERE tenant_id = '{tenant_id}'
              AND {CATALOG_BT}.{SCHEMA_BT}.tenant_row_filter(tenant_id)
            GROUP BY 1
        """)
        result.show()
    except Exception as e:
        print(f"  Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the filter function definition
# MAGIC
# MAGIC Inspect what we deployed so the class can see the IS_MEMBER() logic.

# COMMAND ----------

spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG_BT}.{SCHEMA_BT}.tenant_row_filter").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Column Masking Demo
# MAGIC
# MAGIC The `budget` column in the campaigns table is masked for non-admins.
# MAGIC Running as the current user (admin), you should see real values.

# COMMAND ----------

print("Admin view — budget visible:")
spark.sql(f"""
    SELECT campaign_name, tenant_name, budget
    FROM {CAMPAIGNS_TABLE_BT_FQN}
    LIMIT 5
""").show(truncate=40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What a tenant SP sees
# MAGIC
# MAGIC A non-admin SP calling this query would see `NULL` for `budget`:
# MAGIC
# MAGIC ```
# MAGIC +---------------------------+-------------+-------+
# MAGIC | campaign_name             | tenant_name | budget|
# MAGIC +---------------------------+-------------+-------+
# MAGIC | AT&T Connected Car Q1     | AT&T        |  null |
# MAGIC | JLR EX90 Launch           | JLR         |  null |
# MAGIC +---------------------------+-------------+-------+
# MAGIC ```
# MAGIC
# MAGIC The `mask_budget` function checks `IS_ACCOUNT_ADMIN() OR IS_MEMBER('{admin_group}')`.
# MAGIC Tenant SPs are in tenant groups (e.g. `att-users`), not the admin group.
# MAGIC So they see `NULL` — protecting commercial sensitivity across tenants.

# COMMAND ----------

# Inspect the mask function definition
spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG_BT}.{SCHEMA_BT}.mask_budget").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Grant Tenant SPs Access to Genie Space
# MAGIC
# MAGIC For the SP-per-tenant pattern to work, each tenant SP must have `CAN_USE` on the
# MAGIC Genie Space. Otherwise, `user_context` calls will return 403.

# COMMAND ----------

# Find the Genie Space by name
spaces = list(w.genie.list_spaces())
genie_space = next((s for s in spaces if s.title == GENIE_NAME), None)

if genie_space is None:
    print(f"Genie Space '{GENIE_NAME}' not found — skipping permissions.")
else:
    GENIE_SPACE_ID_LIVE = genie_space.space_id
    print(f"Genie Space: {GENIE_NAME} (id={GENIE_SPACE_ID_LIVE})")

    # Grant CAN_USE to each tenant SP
    for rec in tenant_sp_records:
        try:
            w.api_client.do(
                "PATCH",
                f"/api/2.0/permissions/genie/spaces/{GENIE_SPACE_ID_LIVE}",
                body={
                    "access_control_list": [{
                        "service_principal_name": rec["application_id"],
                        "permission_level": "CAN_USE",
                    }]
                },
            )
            print(f"  Granted CAN_USE to {rec['display_name']} ({rec['tenant_id']})")
        except Exception as e:
            print(f"  WARN ({rec['tenant_id']}): {e}")

    # Grant CAN_MANAGE to admin group
    try:
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/genie/spaces/{GENIE_SPACE_ID_LIVE}",
            body={
                "access_control_list": [{
                    "group_name": ADMIN_GROUP,
                    "permission_level": "CAN_MANAGE",
                }]
            },
        )
        print(f"  Granted CAN_MANAGE to {ADMIN_GROUP}")
    except Exception as e:
        print(f"  WARN (admin group): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Audit Logging via System Tables
# MAGIC
# MAGIC Unity Catalog writes every data access event to `system.access.audit`.
# MAGIC This is how you prove governance is working — or investigate a potential breach.
# MAGIC
# MAGIC > **Note:** Audit events typically have a 15–60 minute lag before appearing.
# MAGIC > Run this after you've exercised the tenant SP calls in `02c_identity`.

# COMMAND ----------

# Recent access events for our tables
try:
    audit_df = spark.sql(f"""
        SELECT
            event_time,
            user_identity.email   AS identity,
            action_name,
            request_params.table_full_name AS table_accessed,
            request_params.operation_name  AS operation
        FROM system.access.audit
        WHERE action_name IN ('commandSubmit', 'runCommand', 'delta.read')
          AND request_params.table_full_name LIKE '{CATALOG}.{SCHEMA}.%'
        ORDER BY event_time DESC
        LIMIT 30
    """)
    audit_df.display()
except Exception as e:
    print(f"Could not query system.access.audit: {e}")
    print("Ensure you have SELECT on system.access.audit (requires account admin or audit log access).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to look for in audit logs
# MAGIC
# MAGIC | Column | What to check |
# MAGIC |---|---|
# MAGIC | `identity` | Should show SP application_id (e.g. `1234567@...`) when called via `user_context` |
# MAGIC | `action_name` | `delta.read` = table scan, `commandSubmit` = SQL execution |
# MAGIC | `table_accessed` | Should be `{catalog}.{schema}.{opportunities_table}` etc. |
# MAGIC
# MAGIC If you see your own admin identity for all events, the `user_context` passthrough isn't
# MAGIC working yet — check that the Genie Space has `CAN_USE` for the tenant SPs (Section 4).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Governance Control | Mechanism | What It Protects |
# MAGIC |---|---|---|
# MAGIC | Table access | `GRANT SELECT` | Can a group query the table at all? |
# MAGIC | Row visibility | `SET ROW FILTER` + `IS_MEMBER()` | Which rows does an identity see? |
# MAGIC | Column sensitivity | `SET MASK` | Can this identity see the budget column? |
# MAGIC | Genie access | `CAN_USE` permission | Can this SP call the Genie Space? |
# MAGIC | Audit trail | `system.access.audit` | Who accessed what, when? |
# MAGIC
# MAGIC All five controls are enforced by **Unity Catalog**, independent of the application.
# MAGIC Even a misconfigured Supervisor Agent cannot bypass UC governance.
# MAGIC
# MAGIC **Next:** Run `02e_evaluate` to measure routing accuracy and verify governance correctness
# MAGIC programmatically via MLflow traces.
