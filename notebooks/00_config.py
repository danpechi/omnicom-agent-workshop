# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Configuration
# MAGIC
# MAGIC **Single source of truth** for all workshop parameters.
# MAGIC
# MAGIC Every other notebook (`01_setup_data`, `02a_setup_and_agent`, `02b_config`, etc.)
# MAGIC starts with `%run ./00_config` to inherit the values defined here.
# MAGIC Edit the widget defaults below or override at runtime — do **not** hardcode
# MAGIC catalog / schema / endpoint names anywhere else in the workshop.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Declares widgets for every configurable name (catalog, schema, volume, LLM endpoint, KA name, Genie name, experiment).
# MAGIC 2. Reads any matching environment variables as overrides (so the same notebook works for bundle deploys and interactive use).
# MAGIC 3. Computes all derived paths and fully-qualified names (volume path, prompt registry FQN, table FQNs, file paths, experiment path).
# MAGIC 4. Prints a summary you can verify before running setup or evaluation.
# MAGIC
# MAGIC **Where these values are also referenced:**
# MAGIC - `databricks.yml` — bundle variables (`var.catalog`, `var.schema`, `var.volume`, `var.llm_endpoint`, `var.ka_name`, `var.genie_name`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widget definitions
# MAGIC
# MAGIC Edit the second argument of each `dbutils.widgets.text(...)` call to change the default.

# COMMAND ----------

import os

# Compute short_name from the current user's email (part before @, dots → underscores).
try:
    _email = spark.sql("SELECT current_user()").collect()[0][0]
    _short_name = _email.split("@")[0].replace(".", "_")
except Exception:
    _short_name = os.getenv("USER", "unknown_user").replace(".", "_")

# Helper: prefer env var, else fallback default.
def _default(env_var: str, fallback: str) -> str:
    return os.getenv(env_var, fallback)


dbutils.widgets.text("catalog",              _default("WORKSHOP_CATALOG",          "users"),                                                  "1. UC Catalog")
dbutils.widgets.text("schema",               _default("WORKSHOP_SCHEMA",           _short_name),                                              "2. UC Schema")
dbutils.widgets.text("volume",               _default("WORKSHOP_VOLUME",           f"{_short_name}_adtech_docs"),                             "3. UC Volume")
dbutils.widgets.text("llm_endpoint",         _default("LLM_ENDPOINT_NAME",         "databricks-claude-sonnet-4-5"),                           "4. LLM serving endpoint")
dbutils.widgets.text("ka_name",              _default("WORKSHOP_KA_NAME",          f"{_short_name}-adtech-ka"),                               "5. Knowledge Assistant name")
dbutils.widgets.text("ka_endpoint",          _default("WORKSHOP_KA_ENDPOINT",      f"{_short_name}-adtech-ka"),                               "6. KA serving endpoint name")
dbutils.widgets.text("genie_name",           _default("WORKSHOP_GENIE_NAME",       f"{_short_name}-adtech-genie"),                            "7. Genie Space name")
dbutils.widgets.text("instructions_name",    _default("WORKSHOP_INSTRUCTIONS",     f"{_short_name}_ka_instructions"),                         "8. Prompt registry name")
dbutils.widgets.text("experiment_name",      _default("WORKSHOP_EXPERIMENT_NAME",  f"{_short_name}-adtech-eval"),                             "9. MLflow experiment subdir")
dbutils.widgets.text("qa_table",             _default("WORKSHOP_QA_TABLE",         f"{_short_name}_sample_qa"),                               "10. UC table: hand-crafted Q&A")
dbutils.widgets.text("eval_table",           _default("WORKSHOP_EVAL_TABLE",       f"{_short_name}_eval_dataset"),                            "11. UC table: eval dataset")
dbutils.widgets.text("opportunities_table",  _default("WORKSHOP_OPPS_TABLE",       f"{_short_name}_opportunities"),                           "12. UC table: opportunities")
dbutils.widgets.text("campaigns_table",      _default("WORKSHOP_CAMPAIGNS_TABLE",  f"{_short_name}_campaigns"),                               "13. UC table: campaigns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve widget values

# COMMAND ----------

CATALOG              = dbutils.widgets.get("catalog").strip()
SCHEMA               = dbutils.widgets.get("schema").strip()
VOLUME               = dbutils.widgets.get("volume").strip()
LLM_ENDPOINT         = dbutils.widgets.get("llm_endpoint").strip()
KA_NAME              = dbutils.widgets.get("ka_name").strip().replace("_", "-")
KA_ENDPOINT          = dbutils.widgets.get("ka_endpoint").strip().replace("_", "-")
GENIE_NAME           = dbutils.widgets.get("genie_name").strip().replace("_", "-")
INSTRUCTIONS_NAME    = dbutils.widgets.get("instructions_name").strip()
EXPERIMENT_NAME      = dbutils.widgets.get("experiment_name").strip()
QA_TABLE             = dbutils.widgets.get("qa_table").strip()
EVAL_TABLE           = dbutils.widgets.get("eval_table").strip()
OPPORTUNITIES_TABLE  = dbutils.widgets.get("opportunities_table").strip()
CAMPAIGNS_TABLE      = dbutils.widgets.get("campaigns_table").strip()

# Validate non-empty
for _name, _val in [
    ("catalog", CATALOG), ("schema", SCHEMA), ("volume", VOLUME),
    ("llm_endpoint", LLM_ENDPOINT), ("ka_name", KA_NAME),
    ("genie_name", GENIE_NAME), ("instructions_name", INSTRUCTIONS_NAME),
    ("experiment_name", EXPERIMENT_NAME), ("qa_table", QA_TABLE),
    ("eval_table", EVAL_TABLE), ("opportunities_table", OPPORTUNITIES_TABLE),
    ("campaigns_table", CAMPAIGNS_TABLE),
]:
    if not _val:
        raise ValueError(f"Widget '{_name}' is empty. Set it before running downstream notebooks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derived paths and fully-qualified names
# MAGIC
# MAGIC Downstream notebooks reference these variables — never recompute them ad-hoc.

# COMMAND ----------

# Volume + file paths (FUSE-mounted at /Volumes/...)
VOLUME_PATH            = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DOCS_PATH              = f"{VOLUME_PATH}/docs"
SAMPLE_QA_PATH         = f"{VOLUME_PATH}/sample_qa.json"
EVAL_DATASET_PATH      = f"{VOLUME_PATH}/eval_dataset.json"

# Fully-qualified UC names
QA_TABLE_FQN           = f"{CATALOG}.{SCHEMA}.{QA_TABLE}"
EVAL_TABLE_FQN         = f"{CATALOG}.{SCHEMA}.{EVAL_TABLE}"
OPPORTUNITIES_TABLE_FQN = f"{CATALOG}.{SCHEMA}.{OPPORTUNITIES_TABLE}"
CAMPAIGNS_TABLE_FQN    = f"{CATALOG}.{SCHEMA}.{CAMPAIGNS_TABLE}"
INSTRUCTIONS_REGISTRY_FQN = f"{CATALOG}.{SCHEMA}.{INSTRUCTIONS_NAME}"

# Backtick-quoted variants for SQL
def _bt(name: str) -> str:
    return f"`{name}`"

CATALOG_BT                  = _bt(CATALOG)
SCHEMA_BT                   = _bt(SCHEMA)
QA_TABLE_BT_FQN             = f"{_bt(CATALOG)}.{_bt(SCHEMA)}.{_bt(QA_TABLE)}"
EVAL_TABLE_BT_FQN           = f"{_bt(CATALOG)}.{_bt(SCHEMA)}.{_bt(EVAL_TABLE)}"
OPPORTUNITIES_TABLE_BT_FQN  = f"{_bt(CATALOG)}.{_bt(SCHEMA)}.{_bt(OPPORTUNITIES_TABLE)}"
CAMPAIGNS_TABLE_BT_FQN      = f"{_bt(CATALOG)}.{_bt(SCHEMA)}.{_bt(CAMPAIGNS_TABLE)}"

# MLflow experiment path under the current user's workspace folder
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
print("  WORKSHOP CONFIG — Omnicom Affinity Hub (AdTech Supervisor Agent)")
print("=" * 70)
print(f"  Catalog              : {CATALOG}")
print(f"  Schema               : {SCHEMA}")
print(f"  Volume               : {VOLUME}")
print(f"  Volume path          : {VOLUME_PATH}")
print()
print(f"  LLM endpoint         : {LLM_ENDPOINT}")
print(f"  KA name              : {KA_NAME}")
print(f"  KA endpoint          : {KA_ENDPOINT}")
print(f"  Genie Space name     : {GENIE_NAME}")
print()
print(f"  Instructions reg.    : {INSTRUCTIONS_REGISTRY_FQN}")
print(f"  Sample Q&A table     : {QA_TABLE_FQN}")
print(f"  Eval table           : {EVAL_TABLE_FQN}")
print(f"  Opportunities table  : {OPPORTUNITIES_TABLE_FQN}")
print(f"  Campaigns table      : {CAMPAIGNS_TABLE_FQN}")
print()
print(f"  Docs path            : {DOCS_PATH}")
print(f"  Sample Q&A path      : {SAMPLE_QA_PATH}")
print(f"  Eval dataset path    : {EVAL_DATASET_PATH}")
print()
print(f"  MLflow experiment    : {EXPERIMENT_PATH}")
print("=" * 70)
