# Omnicom Affinity Hub — Governance & Identity Workshop

A workshop demonstrating how to build a **multi-tenant Supervisor Agent** on Databricks with
real identity passthrough and Unity Catalog governance. Uses the Omnicom Affinity Hub adtech
use case: AT&T, JLR, Pepsi, Ford, and Samsung as tenants.

## Architecture

```
                       ┌─────────────────────────────────────────────┐
                       │         Supervisor Agent (App)               │
                       │                                              │
                       │  1. Extract tenant_id from custom_inputs     │
                       │  2. LLM Router — "ka" (docs) vs "genie" (data) │
                       │  3. Resolve tenant SP application_id         │
                       └───────────┬──────────────────┬──────────────┘
                                   │                  │
              ┌────────────────────▼──┐    ┌──────────▼────────────────────┐
              │  Knowledge Assistant  │    │        Genie Space             │
              │  (shared, all tenants)│    │  + user_context.user_id        │
              │                       │    │    = tenant SP app_id          │
              │  AH methodology       │    │                                │
              │  AT&T account docs    │    │  UC IS_MEMBER() RLS fires:     │
              │  Campaign playbooks   │    │  AT&T SP → AT&T rows only      │
              │  Case studies         │    │  JLR SP → JLR rows only        │
              └───────────────────────┘    └────────────────────────────────┘

Unity Catalog
┌──────────────────────────────────────────────────────────┐
│  Row Filter: tenant_row_filter(tenant_id) via IS_MEMBER() │
│  Column Mask: mask_budget — NULL for non-admins           │
│  Grants: each tenant group has SELECT on their tables     │
│  Audit: system.access.audit logs every query + identity   │
└──────────────────────────────────────────────────────────┘
```

**KA** answers questions from unstructured documents (methodology, playbooks, account info, case studies).
**Genie** answers questions about structured data using natural language SQL, with identity passthrough.
**Supervisor** routes each question to the right sub-agent and injects the tenant SP identity.

## Key Concept: SP-per-Tenant Identity Passthrough

Each tenant has a real Databricks **service principal** added to a tenant-scoped UC group.
When the Supervisor calls Genie, it passes the tenant SP as `user_context.user_id`.
Genie executes the SQL as that SP — so UC's `IS_MEMBER()` row filter fires correctly.

```
Tenant group mapping:
  TEN-001 → {short_name}-att-users  → AT&T SP
  TEN-002 → {short_name}-jlr-users  → JLR SP
  TEN-003 → {short_name}-pepsi-users → Pepsi SP
  ...

Row filter (enforced by UC, not app code):
  IS_ACCOUNT_ADMIN()
  OR IS_MEMBER('{short_name}-omnicom-admin')
  OR (tenant_id = 'TEN-001' AND IS_MEMBER('{short_name}-att-users'))
  OR ...
```

## Multi-User Scoping

All collision-prone resource names are automatically prefixed with the deploying user's
`short_name` (part of email before `@`, dots → underscores). Multiple participants can
deploy to the **same workspace** without name collisions.

| Resource | Naming Pattern | Example (`jane_doe`) |
|---|---|---|
| KA name | `<short_name>-adtech-ka` | `jane_doe-adtech-ka` |
| Genie Space | `<short_name>-adtech-genie` | `jane_doe-adtech-genie` |
| Supervisor endpoint | `<short_name>-adtech-supervisor` | `jane_doe-adtech-supervisor` |
| Volume | `<short_name>_adtech_docs` | `jane_doe_adtech_docs` |
| Tenant groups | `<short_name>-att-users`, `<short_name>-jlr-users`, ... | |
| Tenant SPs | `<short_name>-ten001-sp`, ... | |
| Admin group | `<short_name>-omnicom-admin` | |

**Catalog:** `users` · **Schema:** `<short_name>` (user-scoped, no shared schema collisions).

## Project Structure

```
notebooks/
  00_config.py                 # Single source of truth: catalog, schema, volume, names
  01_setup_data.py             # Docs + tables + groups + tenant SPs + RLS + column masking
  02a_setup_agents.py          # Create KA (shared), grant SP permissions, architecture guide
  02b_tracing_deep_dive.py     # Auto-traces, programmatic trace search, Delta observability
  02c_identity.py              # Frontend→API, 3 identity patterns, SP-per-tenant live demo
  02d_governance.py            # UC grants, RLS verify, column masking, Genie perms, audit logs
  02e_evaluate.py              # Routing accuracy, governance correctness, answer quality

agent_server/
  agent.py                     # Supervisor: tenant_id → SP lookup → route → KA or Genie
  start_server.py              # FastAPI server + Omnicom Affinity Hub chat UI
```

## Workshop Flow

```
00_config → 01_setup_data → 02a_setup_agents → 02b_tracing
                                                      ↓
                              02e_evaluate ← 02d_governance ← 02c_identity
```

## Configuration

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
| `supervisor_endpoint` | `<short_name>-adtech-supervisor` | Supervisor serving endpoint |
| `experiment_name` | `<short_name>-adtech-eval` | MLflow experiment subdir |
| `tenant_sps_table` | `<short_name>_tenant_sps` | UC table: tenant SP lookup |

## Quick Start

1. Run `01_setup_data` — creates docs, tables, UC groups, tenant SPs, RLS, column masking.
2. Run `02a_setup_agents` — creates the shared KA, grants tenant SP permissions.
3. Run `02b_tracing_deep_dive` — explore auto-tracing and programmatic trace search.
4. Run `02c_identity` — live demo of SP-per-tenant identity passthrough.
5. Run `02d_governance` — verify UC grants, RLS, column masking, audit logs.
6. Run `02e_evaluate` — measure routing accuracy and governance correctness.

## Knowledge Sources

### Unstructured Documents (KA) — Shared Across All Tenants

| Document | Content |
|---|---|
| `att_account_overview.md` | AT&T account summary, stakeholders, active/completed engagements |
| `affinity_methodology.md` | AH methodology, Affinity Analysis process, Loop types, node creation |
| `automotive_new_buyer_campaign.md` | AT&T Connected Car campaign playbook by journey stage |
| `new_client_onboarding.md` | Pre-qualification, SOW components, pricing model, go-live checklist |
| `reference_case_studies.md` | JLR EX90 and Pepsi Super Bowl case studies with results |

### Structured Data (Genie) — Tenant-Scoped via RLS

| Table | Rows | Content |
|---|---|---|
| `opportunities` | 15 | Tenant pipeline (AT&T, JLR, Pepsi, Ford, Samsung) with impact/ease scores |
| `campaigns` | 16 | Campaign performance by journey stage (Awareness → Onboarding) and channel |
| `tenant_sps` | 5 | Tenant → SP application_id lookup (used by Supervisor for identity passthrough) |

## Governance Controls

| Control | Mechanism | What It Protects |
|---|---|---|
| Table access | `GRANT SELECT` per tenant group | Can a group query the table at all? |
| Row visibility | `SET ROW FILTER` + `IS_MEMBER()` | Which rows does an identity see? |
| Column sensitivity | `SET MASK mask_budget` | Budget is NULL for non-admins |
| Genie access | `CAN_USE` per tenant SP | Can this SP call the Genie Space? |
| Audit trail | `system.access.audit` | Who accessed what, when? |

## Evaluation Scorers

| Scorer | What It Measures | Target |
|---|---|---|
| Routing accuracy | LLM classifier ka/genie correctness | > 90% |
| Governance correctness | No cross-tenant data leakage | 100% |
| Answer quality (KA) | Key facts present in KA responses | > 75% |
