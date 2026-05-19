# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Configuration
# MAGIC
# MAGIC **Single source of truth** for all workshop parameters.
# MAGIC
# MAGIC Every other notebook starts with `%run ./00_config` to inherit these values.
# MAGIC Edit widget defaults below or override at runtime — do **not** hardcode
# MAGIC catalog / schema / endpoint names anywhere else in the workshop.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Declares widgets for every configurable name.
# MAGIC 2. Reads matching environment variables as overrides (bundle deploy support).
# MAGIC 3. Computes all derived paths and fully-qualified UC names.
# MAGIC 4. Prints a summary you can verify before running setup.

# COMMAND ----------

import os

# Compute short_name from the current user's email (part before @, dots → underscores).
try:
    _email = spark.sql("SELECT current_user()").collect()[0][0]
    _short_name = _email.split("@")[0].replace(".", "_")
except Exception:
    _short_name = os.getenv("USER", "unknown_user").replace(".", "_")

def _default(env_var: str, fallback: str) -> str:
    return os.getenv(env_var, fallback)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widget definitions

# COMMAND ----------

dbutils.widgets.text("catalog",             _default("WORKSHOP_CATALOG",           "users"),                                                  "1. UC Catalog (user-scoped resources)")
dbutils.widgets.text("schema",              _default("WORKSHOP_SCHEMA",            _short_name),                                              "2. UC Schema (user-scoped resources)")
dbutils.widgets.text("volume",              _default("WORKSHOP_VOLUME",            f"{_short_name}_adtech_docs"),                             "3. UC Volume (user-scoped)")
dbutils.widgets.text("data_catalog",        _default("DATA_CATALOG",               "databricks_workshop"),                                    "4. Shared data catalog")
dbutils.widgets.text("data_schema",         _default("DATA_SCHEMA",                "sample"),                                                 "5. Shared data schema")
dbutils.widgets.text("llm_endpoint",        _default("LLM_ENDPOINT_NAME",          "databricks-claude-sonnet-4-5"),                           "6. LLM serving endpoint")
dbutils.widgets.text("ka_name",             _default("WORKSHOP_KA_NAME",           f"{_short_name}-adtech-ka"),                               "7. Knowledge Assistant name")
dbutils.widgets.text("ka_endpoint",         _default("WORKSHOP_KA_ENDPOINT",       f"{_short_name}-adtech-ka"),                               "8. KA serving endpoint name")
dbutils.widgets.text("genie_name",          _default("WORKSHOP_GENIE_NAME",        f"{_short_name}-adtech-genie"),                            "9. Genie Space name")
dbutils.widgets.text("supervisor_endpoint", _default("WORKSHOP_SUPERVISOR_EP",     f"{_short_name}-adtech-supervisor"),                       "10. Supervisor serving endpoint")
dbutils.widgets.text("experiment_name",     _default("WORKSHOP_EXPERIMENT_NAME",   f"{_short_name}-adtech-eval"),                             "11. MLflow experiment subdir")
dbutils.widgets.text("qa_table",            _default("WORKSHOP_QA_TABLE",          f"{_short_name}_sample_qa"),                               "12. UC table: hand-crafted Q&A")
dbutils.widgets.text("eval_table",          _default("WORKSHOP_EVAL_TABLE",        f"{_short_name}_eval_dataset"),                            "13. UC table: eval dataset")
dbutils.widgets.text("tenant_sps_table",    _default("WORKSHOP_TENANT_SPS_TABLE",  f"{_short_name}_tenant_sps"),                              "14. UC table: tenant service principals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve widget values

# COMMAND ----------

CATALOG              = dbutils.widgets.get("catalog").strip()
SCHEMA               = dbutils.widgets.get("schema").strip()
VOLUME               = dbutils.widgets.get("volume").strip()
DATA_CATALOG         = dbutils.widgets.get("data_catalog").strip()
DATA_SCHEMA          = dbutils.widgets.get("data_schema").strip()
LLM_ENDPOINT         = dbutils.widgets.get("llm_endpoint").strip()
KA_NAME              = dbutils.widgets.get("ka_name").strip().replace("_", "-")
KA_ENDPOINT          = dbutils.widgets.get("ka_endpoint").strip().replace("_", "-")
GENIE_NAME           = dbutils.widgets.get("genie_name").strip().replace("_", "-")
SUPERVISOR_ENDPOINT  = dbutils.widgets.get("supervisor_endpoint").strip().replace("_", "-")
EXPERIMENT_NAME      = dbutils.widgets.get("experiment_name").strip()
QA_TABLE             = dbutils.widgets.get("qa_table").strip()
EVAL_TABLE           = dbutils.widgets.get("eval_table").strip()
TENANT_SPS_TABLE     = dbutils.widgets.get("tenant_sps_table").strip()

for _name, _val in [
    ("catalog", CATALOG), ("schema", SCHEMA), ("volume", VOLUME),
    ("data_catalog", DATA_CATALOG), ("data_schema", DATA_SCHEMA),
    ("llm_endpoint", LLM_ENDPOINT), ("ka_name", KA_NAME),
    ("genie_name", GENIE_NAME), ("supervisor_endpoint", SUPERVISOR_ENDPOINT),
    ("experiment_name", EXPERIMENT_NAME),
]:
    if not _val:
        raise ValueError(f"Widget '{_name}' is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derived paths and fully-qualified names

# COMMAND ----------

# Volume + file paths
VOLUME_PATH            = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DOCS_PATH              = f"{VOLUME_PATH}/docs"
SAMPLE_QA_PATH         = f"{VOLUME_PATH}/sample_qa.json"
EVAL_DATASET_PATH      = f"{VOLUME_PATH}/eval_dataset.json"

# Backtick-quoted for SQL
def _bt(name: str) -> str:
    return f"`{name}`"

CATALOG_BT       = _bt(CATALOG)
SCHEMA_BT        = _bt(SCHEMA)
DATA_CATALOG_BT  = _bt(DATA_CATALOG)
DATA_SCHEMA_BT   = _bt(DATA_SCHEMA)

# User-scoped eval/QA tables (written by this workshop user)
QA_TABLE_FQN        = f"{CATALOG}.{SCHEMA}.{QA_TABLE}"
EVAL_TABLE_FQN      = f"{CATALOG}.{SCHEMA}.{EVAL_TABLE}"
TENANT_SPS_TABLE_FQN = f"{CATALOG}.{SCHEMA}.{TENANT_SPS_TABLE}"
QA_TABLE_BT_FQN     = f"{CATALOG_BT}.{SCHEMA_BT}.{_bt(QA_TABLE)}"
EVAL_TABLE_BT_FQN   = f"{CATALOG_BT}.{SCHEMA_BT}.{_bt(EVAL_TABLE)}"
TENANT_SPS_TABLE_BT_FQN = f"{CATALOG_BT}.{SCHEMA_BT}.{_bt(TENANT_SPS_TABLE)}"

# Shared data tables (already exist in databricks_workshop.sample)
DATA_TABLES = {
    "campaign_performance": f"{DATA_CATALOG}.{DATA_SCHEMA}.campaign_performance",
    "campaigns":            f"{DATA_CATALOG}.{DATA_SCHEMA}.campaigns",
    "clients":              f"{DATA_CATALOG}.{DATA_SCHEMA}.clients",
    "creative_assets":      f"{DATA_CATALOG}.{DATA_SCHEMA}.creative_assets",
    "financials":           f"{DATA_CATALOG}.{DATA_SCHEMA}.financials",
}

# Unstructured data: volume path for KA knowledge source
# The KA uses source_type="files" with a /Volumes path.
# If databricks_workshop.sample.unstructured is a Delta table instead,
# update UNSTRUCTURED_PATH or use source_type="table" in 02a_setup_agents.
UNSTRUCTURED_PATH = f"/Volumes/{DATA_CATALOG}/{DATA_SCHEMA}/unstructured"

# UC group names (used in RLS functions and permission grants)
TENANT_GROUPS = {
    "TEN-001": f"{_short_name}-att-users",
    "TEN-002": f"{_short_name}-jlr-users",
    "TEN-003": f"{_short_name}-pepsi-users",
    "TEN-004": f"{_short_name}-ford-users",
    "TEN-005": f"{_short_name}-samsung-users",
}
ADMIN_GROUP = f"{_short_name}-omnicom-admin"

# MLflow experiment
try:
    _username = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    _username = os.getenv("USER", "unknown-user")
EXPERIMENT_PATH = f"/Users/{_username}/{EXPERIMENT_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

print("=" * 70)
print("  WORKSHOP CONFIG — Omnicom Affinity Hub (Governance & Identity)")
print("=" * 70)
print(f"  User catalog/schema  : {CATALOG}.{SCHEMA}")
print(f"  Volume path          : {VOLUME_PATH}")
print()
print(f"  Shared data catalog  : {DATA_CATALOG}.{DATA_SCHEMA}")
print(f"  Shared tables        :")
for tname, fqn in DATA_TABLES.items():
    print(f"    - {fqn}")
print(f"  Unstructured path    : {UNSTRUCTURED_PATH}")
print()
print(f"  LLM endpoint         : {LLM_ENDPOINT}")
print(f"  KA endpoint          : {KA_ENDPOINT}")
print(f"  Genie Space name     : {GENIE_NAME}")
print(f"  Supervisor endpoint  : {SUPERVISOR_ENDPOINT}")
print()
print(f"  Tenant SPs table     : {TENANT_SPS_TABLE_FQN}")
print(f"  UC Groups:")
for tid, grp in TENANT_GROUPS.items():
    print(f"    {tid}: {grp}")
print(f"    Admin: {ADMIN_GROUP}")
print()
print(f"  MLflow experiment    : {EXPERIMENT_PATH}")
print("=" * 70)
