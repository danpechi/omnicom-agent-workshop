# Omnicom Affinity Hub — Supervisor Agent Workshop

A workshop demonstrating how to build, evaluate, and optimize a Databricks Supervisor Agent
for the Omnicom Affinity Hub adtech use case, using MLflow for tracing and evaluation.

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │         Supervisor Agent (App)           │
                        │                                          │
                        │  LLM Router — classifies question type   │
                        │       "ka" (docs) vs "genie" (data)      │
                        └───────────┬──────────────────┬───────────┘
                                    │                  │
               ┌────────────────────▼──┐    ┌──────────▼────────────────────┐
               │  Knowledge Assistant  │    │        Genie Space             │
               │  <short_name>-adtech-ka│   │  <short_name>-adtech-genie    │
               │                       │    │                                │
               │  AT&T account docs    │    │  opportunities table           │
               │  AH methodology       │    │  campaigns table               │
               │  Campaign playbooks   │    │  Natural language SQL          │
               │  Case studies         │    │                                │
               └───────────────────────┘    └────────────────────────────────┘

MLflow Prompt Registry          MLflow Experiment
┌──────────────────────┐       ┌──────────────────────┐
│ <instructions_name>  │       │ Eval runs + traces   │
│   @v1 (baseline)     │       │ V1 vs Optimized      │
│   @optimized (GEPA)  │       └──────────────────────┘
└──────────────────────┘
```

**KA** answers questions from unstructured documents (methodology, playbooks, account info, case studies).
**Genie** answers questions about structured data (opportunity pipeline, campaign performance by journey stage).
The **Supervisor Agent** routes each question to the right sub-agent using an LLM classifier.

## Multi-User Scoping

All collision-prone resource names are automatically prefixed with the deploying user's
`short_name` (the part of their email before `@`, dots replaced by underscores). Multiple
participants can deploy to the **same workspace** without name collisions.

| Resource | Naming Pattern | Example (`jane_doe`) |
|---|---|---|
| KA name | `<short_name>-adtech-ka` | `jane_doe-adtech-ka` |
| Genie Space | `<short_name>-adtech-genie` | `jane_doe-adtech-genie` |
| Experiment | `<short_name>-adtech-eval` | `jane_doe-adtech-eval` |
| Volume | `<short_name>_adtech_docs` | `jane_doe_adtech_docs` |
| Tables | `<short_name>_opportunities`, `<short_name>_campaigns`, etc. | |
| Instructions | `<short_name>_ka_instructions` | `jane_doe_ka_instructions` |

**Catalog:** `users` · **Schema:** `<short_name>` (user-scoped, no shared schema collisions).

## Project Structure

```
notebooks/
  00_config.py                  # Single source of truth: catalog, schema, volume, KA, Genie names
  01_setup_data.py              # AT&T/Omnicom docs, opportunities + campaigns tables, Genie Space
  02a_setup_and_agent.py        # Create KA, register V1 instructions, verify endpoints
  02b_tracing_deep_dive.py      # Supervisor Agent tracing, programmatic search, Delta observability
  02c_evaluate_v1.py            # Evaluate V1 baseline instructions
  02d_optimize_prompt.py        # GEPA instruction optimization
  02e_evaluate_and_compare.py   # Compare V1 vs optimized
  02f_redeploy_app.py           # Apply optimized instructions to live KA

agent_server/
  agent.py                      # Supervisor Agent: LLM router → KA or Genie
  start_server.py               # FastAPI server + Omnicom Affinity Hub chat UI
```

## Workshop Flow

```
00_config  →  01_setup_data  →  02a_setup_ka  →  02b_tracing  →  02c_eval_v1
                                                                        ↓
                                               02f_update_ka  ←  02e_compare  ←  02d_optimize
```

## Configuration — Single Source of Truth

All names are defined in **one** place: `notebooks/00_config.py`.

| Variable | Default | Description |
|---|---|---|
| `catalog` | `users` | Unity Catalog catalog |
| `schema` | `<short_name>` | Unity Catalog schema (user-scoped) |
| `volume` | `<short_name>_adtech_docs` | UC volume for documents and eval data |
| `llm_endpoint` | `databricks-claude-sonnet-4-5` | LLM serving endpoint |
| `ka_name` | `<short_name>-adtech-ka` | Knowledge Assistant name |
| `ka_endpoint` | `<short_name>-adtech-ka` | KA serving endpoint name |
| `genie_name` | `<short_name>-adtech-genie` | Genie Space name |
| `instructions_name` | `<short_name>_ka_instructions` | MLflow Prompt Registry entry |
| `experiment_name` | `<short_name>-adtech-eval` | MLflow experiment subdir |
| `qa_table` | `<short_name>_sample_qa` | UC table for hand-crafted Q&A |
| `eval_table` | `<short_name>_eval_dataset` | UC table for eval dataset |
| `opportunities_table` | `<short_name>_opportunities` | UC table for opportunity pipeline |
| `campaigns_table` | `<short_name>_campaigns` | UC table for campaign performance |

## Quick Start

1. Run `01_setup_data` — creates AT&T/Omnicom documents, opportunities & campaigns tables, and the Genie Space.
2. Run `02a_setup_and_agent` — creates the KA and registers V1 instructions in the Prompt Registry.
3. Add the printed `GENIE_SPACE_ID` to `app.yaml` under `GENIE_SPACE_NAME`.
4. Run `02b` through `02f` sequentially to trace, evaluate, optimize, and redeploy.

## Knowledge Sources

### Unstructured Documents (KA)

Five synthetic Omnicom/AT&T documents uploaded to the UC Volume:

| Document | Content |
|---|---|
| `att_account_overview.md` | AT&T account summary, stakeholders, active/completed engagements |
| `affinity_methodology.md` | AH methodology, Affinity Analysis process, Loop types, node creation |
| `automotive_new_buyer_campaign.md` | AT&T Connected Car campaign playbook by journey stage |
| `new_client_onboarding.md` | Pre-qualification, SOW components, pricing model, go-live checklist |
| `reference_case_studies.md` | JLR EX90 and Pepsi Super Bowl case studies with results |

### Structured Data (Genie)

| Table | Rows | Content |
|---|---|---|
| `opportunities` | 15 | Tenant pipeline (AT&T, JLR, Pepsi, Ford, Samsung) with impact/ease scores |
| `campaigns` | 16 | Campaign performance by journey stage (Awareness → Onboarding) and channel |

## Scorers

| Scorer | What it measures |
|---|---|
| `answer_quality` | Key facts from expected answer are present in the response |
| `Safety` | MLflow built-in safety check |
| `groundedness` | Response cites or references a source document or data table |
| `completeness` | All parts of a multi-part question are addressed |
