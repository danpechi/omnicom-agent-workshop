# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Data: Omnicom Affinity Hub — AdTech Documents, Structured Data & Genie Space
# MAGIC
# MAGIC This notebook generates all seed data for the Omnicom Affinity Hub workshop.
# MAGIC It is designed to auto-run on initial deploy and is fully self-contained.
# MAGIC
# MAGIC **Created artifacts:**
# MAGIC - 5 synthetic Omnicom/AT&T adtech documents (Markdown) in UC Volume
# MAGIC - `opportunities` Unity Catalog table — structured opportunity pipeline data
# MAGIC - `campaigns` Unity Catalog table — campaign performance by journey stage
# MAGIC - Genie Space connected to opportunities + campaigns tables
# MAGIC - `sample_qa.json` (15 hand-crafted Q&A pairs: KA + Genie + mixed)
# MAGIC - `sample_qa` Unity Catalog table
# MAGIC - `eval_dataset.json` (30 evaluation examples: 15 hand-crafted + 15 LLM-generated)
# MAGIC - `eval_dataset` Unity Catalog table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load workshop configuration

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

MODEL = LLM_ENDPOINT

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog / schema / volume / docs directory

# COMMAND ----------

import os

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_BT}.{SCHEMA_BT}.`{VOLUME}`")
os.makedirs(DOCS_PATH, exist_ok=True)
print(f"Volume and docs directory confirmed: {DOCS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Omnicom/AT&T adtech documents
# MAGIC
# MAGIC Five synthetic documents covering core Affinity Hub domains:
# MAGIC AT&T account overview, AH methodology, automotive campaign playbook,
# MAGIC new client onboarding, and reference case studies.

# COMMAND ----------

DOCUMENTS = {}

DOCUMENTS["att_account_overview.md"] = """# AT&T — Account Overview

## Account Summary

| Field | Details |
|-------|---------|
| Account ID | TEN-001 |
| Tenant Name | AT&T |
| Account Tier | Strategic (Tier 1) |
| Relationship Owner | Sarah Chen, SVP Client Partnerships |
| Account Manager | Marcus Rivera |
| Contract Start | January 2022 |
| Annual Revenue | $4.2M |
| Status | Active |

## Current Engagements

### Active Affinity Analyses
1. **Automotive New Buyer Onboarding** — Identifying high-affinity audiences for AT&T connectivity
   bundles targeting customers in the vehicle purchase journey (automotive dealership partnerships).
   Status: In Progress. Expected delivery: Q2 2025.

2. **Business Mobility Expansion** — Affinity loops targeting SMB decision-makers likely to upgrade
   to AT&T Business Unlimited Pro plans. Status: Review phase.

3. **5G Home Internet Adoption** — Geographic and behavioral affinity analysis for residential 5G
   home internet upsell. Status: Active.

### Completed Engagements (Last 12 Months)
- **FirstNet Public Safety** — Affinity analysis identifying first responder adjacent audiences.
  Delivered April 2024. Performance: 34% uplift in qualified leads vs. baseline.
- **Premium TV Bundle Upsell** — Existing customer affinity loops for DIRECTV Stream + AT&T Fiber
  bundle. Delivered January 2024. Conversion rate: 8.2% (benchmark: 5.5%).

## Key Stakeholders

| Name | Title | Role in Engagement |
|------|-------|-------------------|
| Jennifer Walsh | VP Media Strategy | Executive sponsor |
| David Park | Director, Digital Acquisition | Day-to-day client contact |
| Lisa Nguyen | Manager, CRM & Audiences | Data access and approvals |
| Robert Tate | Legal / Privacy Counsel | Data use agreements |

## Account Notes
- AT&T requires all audience segments to be reviewed by Legal/Privacy before activation.
- First-party data sharing governed under the AT&T Data Collaboration Agreement (DCA-2022-AT&T-001).
- Preferred activation platforms: The Trade Desk, Google DV360, Meta.
- Reporting cadence: Monthly executive summary + weekly campaign dashboard access.
- AT&T has standing interest in automotive, business mobility, and residential internet verticals.
"""

DOCUMENTS["affinity_methodology.md"] = """# Affinity Hub Methodology

## What Is Affinity Hub?

Affinity Hub (AH) is Omnicom's proprietary audience intelligence platform. It combines first-party
client data, third-party data partnerships, and behavioral signals to identify high-value audience
segments — called **Affinity Groups** — that are most likely to convert for a client's specific
business objective.

## Core Concepts

### Affinity Analysis
An Affinity Analysis is the foundational engagement type. It answers the question:
*"Which existing customers or prospects have the highest behavioral and attitudinal alignment
with our target product or service?"*

**Process:**
1. **Discovery** — Client defines the target behavior (e.g., "purchase a new vehicle in the next 90 days").
2. **Data Ingestion** — Client CRM data, transactional data, and approved third-party signals are ingested.
3. **Seed Audience Construction** — A seed audience of known converters is built from historical data.
4. **Lookalike Modeling** — AH models expand the seed into a scored prospect universe (Affinity Score 0–100).
5. **Segment Delivery** — Scored segments are delivered to activation platforms via secure clean room.
6. **Measurement** — Post-campaign lift measurement compares conversion rates vs. holdout groups.

### Affinity Loops
An **Affinity Loop** is a continuous optimization cycle that builds on an initial Affinity Analysis.
Rather than a one-time analysis, it incorporates live campaign performance signals to continuously
update and refine the audience model.

**Loop Types:**
| Type | Description | Cadence |
|------|-------------|---------|
| Standard Loop | Monthly model refresh based on campaign response data | Monthly |
| Accelerated Loop | Weekly refresh — used for time-sensitive campaigns (e.g., vehicle launches) | Weekly |
| Always-On Loop | Real-time scoring update tied directly to activation platform API | Continuous |

### Affinity Loop Nodes
A **Loop Node** is a discrete decision point or data feed within an Affinity Loop. Each node
contributes a signal that influences the audience score.

**Node Types:**
- **Behavioral Node** — Web browsing, app usage, search intent data
- **Transactional Node** — Purchase history, loyalty program activity
- **Location Node** — Foot traffic patterns (dealership visits, retail locations)
- **CRM Node** — Client-provided customer attributes and lifecycle stage
- **Partner Node** — Consented third-party data from AH data partners (e.g., Experian, TransUnion)

**Creating a New Node:**
1. Submit a Node Addition Request (NAR) to the AH Platform team.
2. Legal reviews data source for compliance with applicable privacy regulations (CCPA, state laws).
3. Data Engineering builds the ingestion pipeline (SLA: 10 business days for standard nodes).
4. QA validates node output against historical ground truth.
5. Node goes live in next Loop refresh cycle.

## Deliverables by Engagement Type

| Engagement | Deliverables |
|-----------|-------------|
| New Affinity Analysis | Audience scoring report, segment files, activation playbook |
| New Affinity Loop | Continuous scored audience feed, monthly performance dashboard |
| Existing Analysis Update | Refreshed model with new data vintage, delta analysis report |
| New Client Onboarding | Data assessment, DCA, brand guidelines review, SOW |
"""

DOCUMENTS["automotive_new_buyer_campaign.md"] = """# AT&T Automotive New Buyer Onboarding — Campaign Playbook

## Overview

This playbook documents the Affinity Hub strategy and tactics for AT&T's Automotive New Buyer
Onboarding campaign. The goal is to identify consumers who have recently purchased or are actively
considering purchasing a new vehicle, and engage them with AT&T Connected Car and mobility bundle offers.

## Audience Strategy

**Target Persona:** "The Connected Car Buyer"
- Adults 25–54 who have visited an automotive dealership or conducted vehicle research online in the past 60 days
- Households with income $75K+ (higher propensity for connected car upgrades)
- Existing AT&T wireless customers are prioritized for upsell; non-customers targeted for acquisition

**Affinity Seed:** Customers who activated AT&T Connected Car within 90 days of a vehicle purchase (historical data from 2021–2024).

## Journey Stages & Tactics

### Stage 1: Awareness
**Objective:** Build AT&T brand visibility among consumers entering the car-buying journey.

| Channel | Tactic | KPI |
|---------|--------|-----|
| Programmatic Display | AH-scored auto-intender audiences via The Trade Desk | Reach, Frequency |
| YouTube / CTV | 15s and 30s pre-roll, Connected Car features messaging | View-Through Rate |
| Streaming Audio | Spotify + Pandora, commuter daypart targeting | Completion Rate |

**Affinity Node:** Behavioral Node — auto dealership search intent (Google, Autotrader, Cars.com)

### Stage 2: Consideration
**Objective:** Drive engagement with AT&T Connected Car product pages and comparison tools.

| Channel | Tactic | KPI |
|---------|--------|-----|
| Search (Google/Bing) | "Connected car plans" keywords + competitor conquest | CTR, CPC |
| Social (Meta) | Dynamic product ads, customer testimonials | Engagement Rate, Link Clicks |
| Email | Triggered email to AT&T existing customers with auto-intent signal | Open Rate, CTR |

**Affinity Node:** Transactional Node — AT&T device upgrade within 30 days of dealership visit

### Stage 3: Purchase / Conversion
**Objective:** Close the loop — activate at point of vehicle purchase.

| Channel | Tactic | KPI |
|---------|--------|-----|
| Direct Mail | Personalized offers mailed to recently purchased vehicle owners (DMV data) | Response Rate |
| In-Dealership | Co-op program with 120 participating dealerships (tabletop materials, QR codes) | In-store activations |
| Retargeting | Cross-device retargeting of Consideration stage non-converters | Conversion Rate |

**Affinity Node:** Location Node — confirmed dealership visit (foot traffic data, Foursquare)

### Stage 4: Onboarding
**Objective:** Activate Connected Car service within 30 days of vehicle purchase.

| Channel | Tactic | KPI |
|---------|--------|-----|
| CRM / Email | Welcome series: 3-email onboarding sequence | Activation Rate |
| Push Notification | AT&T app — vehicle setup wizard prompts | Completion Rate |
| Customer Care | Proactive outreach from AT&T Connected Car specialists | NPS |

## Performance Benchmarks (Historical)
| Metric | AT&T Baseline | AH Optimized | Lift |
|--------|--------------|-------------|------|
| Connected Car Activation Rate | 12% | 19% | +58% |
| Cost Per Activation | $42 | $27 | -36% |
| 90-Day Retention | 71% | 84% | +18% |
| Average Revenue Per User (ARPU) | $18/mo | $23/mo | +28% |
"""

DOCUMENTS["new_client_onboarding.md"] = """# New Client Onboarding Guide — Omnicom Affinity Hub

## Overview

This guide covers the end-to-end process for onboarding a new client to the Affinity Hub platform.
The full onboarding process takes 6–10 weeks from initial scope agreement to first audience delivery.

## Phase 1: Pre-Qualification (Weeks 1–2)

### Required Documentation
All new clients must provide the following before a Statement of Work (SOW) can be issued:

| Document | Purpose | Who Provides |
|----------|---------|--------------|
| Request for Proposal (RFP) | Defines client's objectives, data assets, and success metrics | Client |
| Data Inventory | List of first-party data assets (CRM, transactional, website) | Client |
| Privacy Policy & Data Governance Summary | Confirms data collection practices | Client |
| Sample Data (anonymized) | Validates data quality and volume sufficiency | Client |
| NDA | Protects both parties during assessment | Both parties |

### Assessment Criteria
AH evaluates new clients on:
- **Data Volume:** Minimum 50,000 seed audience records required for reliable modeling.
- **Data Recency:** Transactional data must include at least 12 months of history.
- **Objective Clarity:** Business objective must be quantifiable (e.g., "increase product activations by 20%").
- **Activation Readiness:** Client must have access to at least one supported activation platform (The Trade Desk, DV360, Meta, Amazon DSP, LiveRamp).

## Phase 2: Scoping & Contracting (Weeks 2–4)

### Statement of Work (SOW) Components
| Section | Content |
|---------|---------|
| Engagement Overview | Objectives, deliverables, timeline |
| Data Requirements | Data sources, ingestion method, refresh cadence |
| Modeling Approach | Analysis type (one-time vs. loop), node configuration |
| Pricing | Fee structure (see Pricing Model below) |
| Measurement Plan | KPIs, holdout methodology, reporting cadence |
| Data Use Agreement (DCA) | Data rights, residency, retention, deletion schedule |

### Pricing Model
| Engagement Type | Base Fee | Variable Component |
|----------------|----------|--------------------|
| New Affinity Analysis (one-time) | $85,000 | + $5,000 per additional segment |
| Standard Affinity Loop | $45,000 setup + $12,000/month | Volume discounts at 3, 6, 12-month terms |
| Accelerated Loop | $55,000 setup + $18,000/month | — |
| Existing Analysis Update | $35,000 | — |
| New Client Onboarding (standalone) | $15,000 | Waived for contracts > $150K |

## Phase 3: Technical Onboarding (Weeks 3–8)

### Data Ingestion
1. AH Data Engineering provisions a secure SFTP endpoint or clean room connector.
2. Client uploads sample data; AH runs automated data quality checks.
3. Data pipeline built and validated (SLA: 15 business days).
4. Privacy review confirms data fields comply with DCA terms.

### Brand Guidelines Review
- AH Creative team reviews client brand guidelines before any audience activation materials are produced.
- Required brand assets: logo files (SVG/EPS), color palette (HEX/Pantone), typography guide, messaging framework.
- Creative brief template is completed jointly by AH and client during kick-off meeting.

### Platform Connections
AH supports direct API integration with:
- The Trade Desk (TTD) — preferred for programmatic display and CTV
- Google DV360 — preferred for YouTube and Display
- Meta Ads Manager — social audiences
- Amazon DSP — e-commerce and Prime audience reach
- LiveRamp — cross-platform identity resolution

## Phase 4: Launch & Measurement (Weeks 6–10)

### Go-Live Checklist
- [ ] Seed audience validated (min 50K records, max 20% suppression rate)
- [ ] Affinity model trained and QA'd (AUC > 0.70 required)
- [ ] Audience segments delivered to activation platform
- [ ] Measurement pixels / conversion tags live
- [ ] Holdout group defined (minimum 10% of reachable universe)
- [ ] Client reporting dashboard live

### Reporting Cadence
- **Weekly:** Campaign performance dashboard (self-serve)
- **Monthly:** Executive summary — reach, frequency, conversion lift, ROAS
- **Quarterly:** Model performance review + affinity score refresh
"""

DOCUMENTS["reference_case_studies.md"] = """# Affinity Hub Reference Case Studies

## Case Study 1: JLR — Jaguar Land Rover

**Engagement Type:** New Affinity Analysis → Ongoing Standard Loop
**Vertical:** Automotive (Luxury)
**Duration:** April 2023 – Present

### Challenge
JLR needed to identify conquest audiences — non-JLR owners with high affinity for luxury vehicle
ownership — ahead of the launch of the all-electric Jaguar EX90.

### Approach
1. AH built a seed audience from JLR's CRM: 82,000 customers who had purchased a Range Rover or
   Jaguar in the prior 3 years.
2. Key nodes deployed:
   - Behavioral: Luxury automotive content consumption (Car and Driver, Road & Track)
   - Transactional: Luxury goods purchases ($5K+ single transactions)
   - Location: Luxury dealership visits and high-end retail (Nordstrom, Apple Store)
3. Model delivered a scored universe of 2.4 million US households, ranked by EV affinity decile.

### Results
| Metric | Control Group | AH-Targeted Group | Lift |
|--------|--------------|------------------|------|
| Test Drive Requests | 1.2% | 3.8% | +217% |
| Vehicle Configurator Completions | 0.4% | 1.7% | +325% |
| Dealer Visit (tracked) | 0.8% | 2.1% | +163% |
| Cost per Qualified Lead | $185 | $67 | -64% |

### Key Learnings
- Luxury goods transactional data was the highest-performing single node (node importance score: 0.38).
- Geographic clustering around ZIP codes with median household income >$150K improved model precision by 22%.
- Standard Loop monthly refreshes maintained performance stability over 12+ months without model drift.

---

## Case Study 2: Pepsi — Next-Gen Sports Fan Engagement

**Engagement Type:** New Affinity Analysis + Accelerated Loop
**Vertical:** Consumer Packaged Goods (CPG)
**Duration:** August 2023 – February 2024 (Super Bowl campaign cycle)

### Challenge
Pepsi needed to identify "Next-Gen Sports Fans" — 18–34 year olds who actively follow multiple
sports leagues and have high CPG brand engagement — for Super Bowl LVIII sponsorship activation.

### Approach
1. Seed audience: 230,000 purchasers of Pepsi products who self-reported sports fan identity
   in loyalty program surveys.
2. Key nodes:
   - Behavioral: Sports media consumption across streaming (ESPN+, Peacock, Max), social sports follows
   - Transactional: Convenience store and grocery purchases with sports event correlation
   - Partner Node: Ticketmaster / SeatGeek concert and live event data (consented)
3. Accelerated Loop (weekly refresh) used given the 6-month campaign sprint to Super Bowl.

### Results
| Metric | Benchmark | Actual | Delta |
|--------|-----------|--------|-------|
| Ad Recall (brand survey) | 32% | 51% | +59% |
| Purchase Intent | 24% | 38% | +58% |
| Social Amplification (shares/engagement) | 1.8% ER | 4.2% ER | +133% |
| Incremental Cases Sold (IRI attribution) | — | +1.4M cases | — |

### Key Learnings
- Live event data (Ticketmaster) was a breakthrough node — fans who attended live events in the prior
  90 days were 3.1x more likely to engage with Pepsi content.
- Accelerated weekly loop maintained audience freshness through the dynamic Super Bowl media cycle.
- Social amplification from AH-targeted segments generated earned media value of $8.2M vs. $1.1M for non-targeted.
"""

import os
for fname, content in DOCUMENTS.items():
    fpath = os.path.join(DOCS_PATH, fname)
    with open(fpath, "w") as f:
        f.write(content)

print(f"Wrote {len(DOCUMENTS)} Omnicom/AT&T documents to {DOCS_PATH}:")
for fname in DOCUMENTS:
    size = len(DOCUMENTS[fname])
    print(f"  - {fname}  ({size:,} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Opportunities table (structured data for Genie)

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

OPPORTUNITIES = [
    # AT&T opportunities
    Row(opportunity_id="OPP-001", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_type="Existing Affinity Analysis Update", title="Automotive New Buyer Onboarding — Model Refresh",
        description="Refresh the AT&T Automotive New Buyer affinity model with 2024 vehicle purchase data and updated behavioral nodes. Add new EV purchase intent signal.",
        impact_score=0.88, ease_score=0.75, status="Active",
        created_date=date(2024, 11, 1), updated_date=date(2025, 2, 15)),
    Row(opportunity_id="OPP-002", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_type="New Affinity Analysis", title="Business Mobility — SMB Decision Maker Targeting",
        description="Identify SMB owners and decision-makers with high propensity to upgrade AT&T business wireless plans. Use transactional + firmographic nodes.",
        impact_score=0.91, ease_score=0.62, status="Active",
        created_date=date(2024, 12, 5), updated_date=date(2025, 1, 20)),
    Row(opportunity_id="OPP-003", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_type="New Affinity Analysis", title="5G Home Internet — Residential Upsell",
        description="Geo-behavioral affinity analysis for residential 5G home internet upsell in markets where AT&T 5G coverage exceeds 80% of households.",
        impact_score=0.79, ease_score=0.81, status="Active",
        created_date=date(2025, 1, 10), updated_date=date(2025, 3, 2)),
    Row(opportunity_id="OPP-004", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_type="New Affinity Analysis", title="FirstNet Public Safety — First Responder Adjacent",
        description=None,
        impact_score=0.72, ease_score=0.55, status="Draft",
        created_date=date(2025, 2, 20), updated_date=date(2025, 2, 20)),
    Row(opportunity_id="OPP-005", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_type="Existing Affinity Analysis Update", title="Premium TV Bundle Upsell — Loop Refresh",
        description="Annual refresh of the DIRECTV Stream + AT&T Fiber bundle affinity loop with updated streaming behavior signals.",
        impact_score=0.65, ease_score=0.88, status="Pending",
        created_date=date(2025, 3, 1), updated_date=date(2025, 3, 1)),

    # JLR opportunities
    Row(opportunity_id="OPP-006", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_type="Existing Affinity Analysis Update", title="EX90 Launch — Conquest Audience Expansion",
        description="Expand the JLR EX90 conquest audience model to include new EV early adopter signals from 2024 EV purchase data. Target: 3.5M scored households.",
        impact_score=0.94, ease_score=0.70, status="Active",
        created_date=date(2024, 10, 15), updated_date=date(2025, 2, 28)),
    Row(opportunity_id="OPP-007", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_type="New Affinity Analysis", title="Defender — Off-Road Adventure Enthusiast",
        description=None,
        impact_score=0.83, ease_score=0.60, status="Draft",
        created_date=date(2025, 1, 25), updated_date=date(2025, 1, 25)),
    Row(opportunity_id="OPP-008", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_type="New Affinity Analysis", title="Range Rover — Ultra-High-Net-Worth Retention",
        description="Identify Range Rover owners at risk of switching to competitor luxury brands. Use luxury lifestyle and financial behavior nodes to score retention risk.",
        impact_score=0.87, ease_score=0.52, status="Pending",
        created_date=date(2025, 2, 10), updated_date=date(2025, 3, 5)),

    # Pepsi opportunities
    Row(opportunity_id="OPP-009", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_type="Existing Affinity Analysis Update", title="Next-Gen Sports Fan — Post-Super Bowl Refresh",
        description="Refresh the Pepsi Next-Gen Sports Fan loop with Super Bowl LVIII engagement data. Extend to include NBA Playoffs and Copa América audiences.",
        impact_score=0.76, ease_score=0.85, status="Active",
        created_date=date(2025, 2, 15), updated_date=date(2025, 3, 10)),
    Row(opportunity_id="OPP-010", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_type="New Affinity Analysis", title="Pepsi Zero Sugar — Health-Conscious Switcher",
        description="Identify diet soda drinkers and health-conscious beverage consumers with high affinity for switching to Pepsi Zero Sugar. CPG transactional + wellness behavioral nodes.",
        impact_score=0.69, ease_score=0.77, status="Active",
        created_date=date(2024, 12, 1), updated_date=date(2025, 1, 15)),
    Row(opportunity_id="OPP-011", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_type="New Affinity Analysis", title="Gatorade — Youth Athlete Parent Targeting",
        description=None,
        impact_score=0.71, ease_score=0.68, status="Draft",
        created_date=date(2025, 3, 5), updated_date=date(2025, 3, 5)),

    # Ford opportunities
    Row(opportunity_id="OPP-012", tenant_id="TEN-004", tenant_name="Ford",
        opportunity_type="New Client", title="Ford Pro — Commercial Fleet Operator Acquisition",
        description="New client engagement. Identify commercial fleet operators (1–50 vehicles) with high propensity to transition to Ford Pro electric work vehicles.",
        impact_score=0.92, ease_score=0.45, status="Pending",
        created_date=date(2025, 1, 5), updated_date=date(2025, 2, 20)),
    Row(opportunity_id="OPP-013", tenant_id="TEN-004", tenant_name="Ford",
        opportunity_type="New Client", title="Mustang Mach-E — EV Consideration Audience",
        description=None,
        impact_score=0.80, ease_score=0.50, status="Draft",
        created_date=date(2025, 2, 1), updated_date=date(2025, 2, 1)),

    # Samsung opportunities
    Row(opportunity_id="OPP-014", tenant_id="TEN-005", tenant_name="Samsung",
        opportunity_type="New Client", title="Galaxy S25 — iPhone Switcher Acquisition",
        description="New client onboarding. Build affinity model targeting iPhone users most likely to switch to Galaxy S25 based on device lifecycle, satisfaction signals, and innovation affinity.",
        impact_score=0.95, ease_score=0.40, status="Pending",
        created_date=date(2025, 1, 20), updated_date=date(2025, 3, 1)),
    Row(opportunity_id="OPP-015", tenant_id="TEN-005", tenant_name="Samsung",
        opportunity_type="New Affinity Analysis", title="Samsung Home Appliances — New Homeowner Targeting",
        description="Identify recent home purchasers (0–18 months) with high propensity for premium appliance purchase. New homeowner + home improvement behavioral node package.",
        impact_score=0.74, ease_score=0.72, status="Active",
        created_date=date(2024, 11, 15), updated_date=date(2025, 2, 5)),
]

opp_df = spark.createDataFrame(OPPORTUNITIES)
opp_df.write.mode("overwrite").saveAsTable(OPPORTUNITIES_TABLE_BT_FQN)
print(f"Wrote table {OPPORTUNITIES_TABLE_FQN} ({opp_df.count()} rows)")
opp_df.show(5, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Campaigns table (structured data for Genie)

# COMMAND ----------

CAMPAIGNS = [
    # AT&T — Automotive New Buyer
    Row(campaign_id="CAM-001", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — Awareness",
        journey_stage="Awareness", channel="Programmatic Display",
        budget=250000.0, impressions=42000000, conversions=8400,
        conversion_rate=0.02, start_date=date(2024, 9, 1), end_date=date(2024, 11, 30)),
    Row(campaign_id="CAM-002", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — YouTube CTV",
        journey_stage="Awareness", channel="CTV / YouTube",
        budget=180000.0, impressions=9500000, conversions=3800,
        conversion_rate=0.04, start_date=date(2024, 9, 1), end_date=date(2024, 11, 30)),
    Row(campaign_id="CAM-003", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — Search",
        journey_stage="Consideration", channel="Search",
        budget=320000.0, impressions=1800000, conversions=28800,
        conversion_rate=0.016, start_date=date(2024, 9, 15), end_date=date(2024, 12, 15)),
    Row(campaign_id="CAM-004", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — Meta Social",
        journey_stage="Consideration", channel="Social",
        budget=210000.0, impressions=22000000, conversions=17600,
        conversion_rate=0.008, start_date=date(2024, 10, 1), end_date=date(2024, 12, 31)),
    Row(campaign_id="CAM-005", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — Direct Mail",
        journey_stage="Purchase", channel="Direct Mail",
        budget=95000.0, impressions=380000, conversions=11400,
        conversion_rate=0.03, start_date=date(2024, 11, 1), end_date=date(2025, 1, 31)),
    Row(campaign_id="CAM-006", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-001", campaign_name="AT&T Connected Car — Onboarding Email",
        journey_stage="Onboarding", channel="Email",
        budget=18000.0, impressions=125000, conversions=23750,
        conversion_rate=0.19, start_date=date(2024, 11, 15), end_date=date(2025, 2, 28)),

    # AT&T — Business Mobility
    Row(campaign_id="CAM-007", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-002", campaign_name="AT&T Business Mobility — SMB Display",
        journey_stage="Awareness", channel="Programmatic Display",
        budget=190000.0, impressions=28000000, conversions=5600,
        conversion_rate=0.002, start_date=date(2025, 1, 15), end_date=date(2025, 4, 15)),
    Row(campaign_id="CAM-008", tenant_id="TEN-001", tenant_name="AT&T",
        opportunity_id="OPP-002", campaign_name="AT&T Business Mobility — LinkedIn",
        journey_stage="Consideration", channel="Social",
        budget=145000.0, impressions=4200000, conversions=12600,
        conversion_rate=0.003, start_date=date(2025, 1, 15), end_date=date(2025, 4, 15)),

    # JLR — EX90
    Row(campaign_id="CAM-009", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_id="OPP-006", campaign_name="JLR EX90 — Conquest Display",
        journey_stage="Awareness", channel="Programmatic Display",
        budget=420000.0, impressions=61000000, conversions=18300,
        conversion_rate=0.003, start_date=date(2024, 10, 1), end_date=date(2025, 1, 31)),
    Row(campaign_id="CAM-010", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_id="OPP-006", campaign_name="JLR EX90 — Test Drive Retargeting",
        journey_stage="Consideration", channel="Programmatic Display",
        budget=85000.0, impressions=3200000, conversions=38400,
        conversion_rate=0.012, start_date=date(2024, 11, 1), end_date=date(2025, 1, 31)),
    Row(campaign_id="CAM-011", tenant_id="TEN-002", tenant_name="JLR",
        opportunity_id="OPP-006", campaign_name="JLR EX90 — Test Drive Direct Mail",
        journey_stage="Purchase", channel="Direct Mail",
        budget=130000.0, impressions=520000, conversions=19760,
        conversion_rate=0.038, start_date=date(2024, 12, 1), end_date=date(2025, 2, 28)),

    # Pepsi — Next-Gen Sports Fan
    Row(campaign_id="CAM-012", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_id="OPP-009", campaign_name="Pepsi Super Bowl — Social Amplification",
        journey_stage="Awareness", channel="Social",
        budget=650000.0, impressions=140000000, conversions=5600000,
        conversion_rate=0.04, start_date=date(2024, 12, 1), end_date=date(2025, 2, 10)),
    Row(campaign_id="CAM-013", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_id="OPP-009", campaign_name="Pepsi Super Bowl — CTV Pre-Roll",
        journey_stage="Awareness", channel="CTV / YouTube",
        budget=890000.0, impressions=55000000, conversions=2750000,
        conversion_rate=0.05, start_date=date(2025, 1, 15), end_date=date(2025, 2, 10)),
    Row(campaign_id="CAM-014", tenant_id="TEN-003", tenant_name="Pepsi",
        opportunity_id="OPP-009", campaign_name="Pepsi Super Bowl — Retail Display",
        journey_stage="Purchase", channel="Programmatic Display",
        budget=320000.0, impressions=18000000, conversions=1080000,
        conversion_rate=0.06, start_date=date(2025, 1, 20), end_date=date(2025, 2, 10)),

    # Samsung — Home Appliances
    Row(campaign_id="CAM-015", tenant_id="TEN-005", tenant_name="Samsung",
        opportunity_id="OPP-015", campaign_name="Samsung Home — New Homeowner Email",
        journey_stage="Awareness", channel="Email",
        budget=42000.0, impressions=280000, conversions=39200,
        conversion_rate=0.14, start_date=date(2025, 1, 1), end_date=date(2025, 3, 31)),
    Row(campaign_id="CAM-016", tenant_id="TEN-005", tenant_name="Samsung",
        opportunity_id="OPP-015", campaign_name="Samsung Home — Search + Display Retarget",
        journey_stage="Consideration", channel="Search",
        budget=115000.0, impressions=2100000, conversions=31500,
        conversion_rate=0.015, start_date=date(2025, 1, 15), end_date=date(2025, 4, 15)),
]

cam_df = spark.createDataFrame(CAMPAIGNS)
cam_df.write.mode("overwrite").saveAsTable(CAMPAIGNS_TABLE_BT_FQN)
print(f"Wrote table {CAMPAIGNS_TABLE_FQN} ({cam_df.count()} rows)")
cam_df.show(5, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Genie Space
# MAGIC
# MAGIC Connect the Genie Space to the opportunities and campaigns tables
# MAGIC so users can ask natural language questions about the structured data.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Find a running SQL warehouse
warehouses = sorted(
    [wh for wh in w.warehouses.list()],
    key=lambda wh: 0 if (wh.state and wh.state.name == "RUNNING") else 1
)
if not warehouses:
    raise RuntimeError("No SQL warehouses found. Create a warehouse and retry.")

warehouse_id = warehouses[0].id
print(f"Using warehouse: {warehouses[0].name} ({warehouse_id})")

# Check if a Genie Space with this name already exists
existing_spaces = list(w.genie.list_spaces())
existing = next((s for s in existing_spaces if s.title == GENIE_NAME), None)

if existing:
    GENIE_SPACE_ID = existing.space_id
    print(f"Genie Space already exists: {GENIE_NAME} (id={GENIE_SPACE_ID})")
else:
    space = w.genie.create_space(
        title=GENIE_NAME,
        description=f"Omnicom Affinity Hub — AdTech structured data (campaigns, clients, financials) for {_short_name}",
        warehouse_id=warehouse_id,
        table_identifiers=list(DATA_TABLES.values()),
    )
    GENIE_SPACE_ID = space.space_id
    print(f"Created Genie Space: {GENIE_NAME} (id={GENIE_SPACE_ID})")

print(f"\nGenie Space ID: {GENIE_SPACE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create UC Groups (one per tenant + admin)
# MAGIC
# MAGIC These groups control who can query which tenant's data.
# MAGIC Row-level security (Section 8) uses `IS_MEMBER()` against these group names.

# COMMAND ----------

# TENANT_GROUPS and ADMIN_GROUP are already defined in 00_config
# (loaded via %run ./00_config above)

for group_name in [*TENANT_GROUPS.values(), ADMIN_GROUP]:
    try:
        w.groups.create(display_name=group_name)
        print(f"Created group: {group_name}")
    except Exception as e:
        if "already exists" in str(e).lower() or "conflict" in str(e).lower():
            print(f"Group already exists: {group_name}")
        else:
            print(f"WARN: Could not create group {group_name}: {e}")

print(f"\nGroups ready: {len(TENANT_GROUPS)} tenant groups + 1 admin group")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Service Principals per Tenant
# MAGIC
# MAGIC Each tenant gets a dedicated service principal (SP) added to its group.
# MAGIC The Supervisor Agent resolves `tenant_id → SP application_id` and passes it
# MAGIC as `user_context` to Genie, so Unity Catalog row filters fire against that SP identity.

# COMMAND ----------

tenant_sp_records = []

for tenant_id, group_name in TENANT_GROUPS.items():
    sp_display_name = f"{_short_name}-{tenant_id.lower().replace('-', '')}-sp"
    try:
        sp = w.service_principals.create(display_name=sp_display_name)
        print(f"Created SP: {sp_display_name} (id={sp.id}, app_id={sp.application_id})")
    except Exception as e:
        if "already exists" in str(e).lower() or "conflict" in str(e).lower():
            # Find existing SP
            existing = next(
                (s for s in w.service_principals.list(filter=f"displayName eq \"{sp_display_name}\"")
                 if s.display_name == sp_display_name),
                None
            )
            if existing:
                sp = existing
                print(f"SP already exists: {sp_display_name} (id={sp.id})")
            else:
                print(f"WARN: Could not create or find SP {sp_display_name}: {e}")
                continue
        else:
            print(f"WARN: Could not create SP {sp_display_name}: {e}")
            continue

    # Add SP to its tenant group
    try:
        group_list = list(w.groups.list(filter=f"displayName eq \"{group_name}\""))
        group = next((g for g in group_list if g.display_name == group_name), None)
        if group:
            w.groups.patch(
                id=group.id,
                operations=[{
                    "op": "add",
                    "path": "members",
                    "value": [{"value": str(sp.id)}],
                }],
            )
            print(f"  Added to group: {group_name}")
        else:
            print(f"  WARN: Group not found: {group_name}")
    except Exception as e:
        print(f"  WARN: Could not add SP to group: {e}")

    tenant_sp_records.append({
        "tenant_id": tenant_id,
        "sp_id": str(sp.id),
        "application_id": str(sp.application_id),
        "display_name": sp_display_name,
        "group_name": group_name,
    })

print(f"\nCreated {len(tenant_sp_records)} tenant service principals.")

# Persist SP lookup table for the Supervisor Agent
sp_df = spark.createDataFrame(tenant_sp_records)
sp_df.write.mode("overwrite").saveAsTable(TENANT_SPS_TABLE_BT_FQN)
print(f"Wrote table {TENANT_SPS_TABLE_FQN} ({sp_df.count()} rows)")
sp_df.show(truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Row-Level Security on opportunities + campaigns
# MAGIC
# MAGIC The `tenant_row_filter` function uses `IS_MEMBER()` to check group membership.
# MAGIC UC evaluates this at query time as the identity making the request — either a
# MAGIC real user or the tenant service principal passed via `user_context`.

# COMMAND ----------

# Build the IS_MEMBER clause dynamically from TENANT_GROUPS
_member_clauses = "\n    OR ".join([
    f"(tenant_id = '{tid}' AND IS_MEMBER('{grp}'))"
    for tid, grp in TENANT_GROUPS.items()
])

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_BT}.{SCHEMA_BT}.tenant_row_filter(tenant_id STRING)
RETURN IS_ACCOUNT_ADMIN()
    OR IS_MEMBER('{ADMIN_GROUP}')
    OR {_member_clauses}
""")
print(f"Created row filter function: {CATALOG}.{SCHEMA}.tenant_row_filter")

# Apply to opportunities table
spark.sql(f"""
ALTER TABLE {OPPORTUNITIES_TABLE_BT_FQN}
  SET ROW FILTER {CATALOG_BT}.{SCHEMA_BT}.tenant_row_filter ON (tenant_id)
""")
print(f"Applied row filter to: {OPPORTUNITIES_TABLE_FQN}")

# Apply to campaigns table
spark.sql(f"""
ALTER TABLE {CAMPAIGNS_TABLE_BT_FQN}
  SET ROW FILTER {CATALOG_BT}.{SCHEMA_BT}.tenant_row_filter ON (tenant_id)
""")
print(f"Applied row filter to: {CAMPAIGNS_TABLE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Column Masking — budget is admin-only
# MAGIC
# MAGIC Non-admin users (including tenant SPs) see `NULL` for the `budget` column
# MAGIC in the campaigns table. Only `omnicom-admin` group members see real values.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_BT}.{SCHEMA_BT}.mask_budget(budget DOUBLE)
RETURN CASE
  WHEN IS_ACCOUNT_ADMIN() OR IS_MEMBER('{ADMIN_GROUP}') THEN budget
  ELSE NULL
END
""")
print(f"Created column mask: {CATALOG}.{SCHEMA}.mask_budget")

spark.sql(f"""
ALTER TABLE {CAMPAIGNS_TABLE_BT_FQN}
  ALTER COLUMN budget SET MASK {CATALOG_BT}.{SCHEMA_BT}.mask_budget
""")
print(f"Applied budget mask to: {CAMPAIGNS_TABLE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Hand-crafted Q&A pairs (15 examples)
# MAGIC
# MAGIC Mix of KA (document-based), Genie (data-based), and mixed questions
# MAGIC covering the full supervisor agent routing surface.

# COMMAND ----------

import json

SAMPLE_QA = [
    # KA-focused (unstructured document questions)
    {
        "qa_id": "QA-001",
        "question": "What is the Affinity Hub methodology for identifying high-value audience segments?",
        "expected_answer": "Affinity Hub uses a 6-step process: (1) Discovery — define the target behavior, (2) Data Ingestion — ingest client CRM and third-party data, (3) Seed Audience Construction — build a seed from known converters, (4) Lookalike Modeling — score a prospect universe 0–100, (5) Segment Delivery — deliver to activation platforms via clean room, (6) Measurement — post-campaign lift vs. holdout group.",
        "source_doc": "affinity_methodology.md",
        "category": "methodology",
        "route": "ka",
    },
    {
        "qa_id": "QA-002",
        "question": "What is an Affinity Loop and what are the different types?",
        "expected_answer": "An Affinity Loop is a continuous optimization cycle that builds on an initial Affinity Analysis, incorporating live campaign signals to continuously update the audience model. The three types are: Standard Loop (monthly refresh), Accelerated Loop (weekly refresh for time-sensitive campaigns), and Always-On Loop (real-time scoring via activation platform API).",
        "source_doc": "affinity_methodology.md",
        "category": "methodology",
        "route": "ka",
    },
    {
        "qa_id": "QA-003",
        "question": "What are the steps to create a new Affinity Loop node?",
        "expected_answer": "The 5 steps to create a new node are: (1) Submit a Node Addition Request (NAR) to the AH Platform team, (2) Legal reviews data source for CCPA/privacy compliance, (3) Data Engineering builds the ingestion pipeline (SLA: 10 business days), (4) QA validates node output against historical ground truth, (5) Node goes live in the next Loop refresh cycle.",
        "source_doc": "affinity_methodology.md",
        "category": "methodology",
        "route": "ka",
    },
    {
        "qa_id": "QA-004",
        "question": "What documentation is required from a new client before a Statement of Work can be issued?",
        "expected_answer": "New clients must provide: (1) Request for Proposal (RFP), (2) Data Inventory listing first-party data assets, (3) Privacy Policy & Data Governance Summary, (4) Sample anonymized data for quality validation, and (5) a signed NDA.",
        "source_doc": "new_client_onboarding.md",
        "category": "onboarding",
        "route": "ka",
    },
    {
        "qa_id": "QA-005",
        "question": "What is the minimum seed audience size required for a new Affinity Analysis?",
        "expected_answer": "A minimum of 50,000 seed audience records is required for reliable modeling. Additionally, transactional data must include at least 12 months of history, and the client must have access to at least one supported activation platform.",
        "source_doc": "new_client_onboarding.md",
        "category": "onboarding",
        "route": "ka",
    },
    {
        "qa_id": "QA-006",
        "question": "What channels are recommended for the Consideration stage of the AT&T Automotive New Buyer campaign?",
        "expected_answer": "For the Consideration stage, the recommended channels are: Search (Google/Bing) using connected car keywords and competitor conquest, Social (Meta) with dynamic product ads and customer testimonials, and Email triggered to existing AT&T customers with auto-intent signals.",
        "source_doc": "automotive_new_buyer_campaign.md",
        "category": "campaign_tactics",
        "route": "ka",
    },
    {
        "qa_id": "QA-007",
        "question": "What performance lift did Affinity Hub achieve for AT&T Connected Car activations?",
        "expected_answer": "Affinity Hub achieved a 58% lift in Connected Car Activation Rate (from 12% baseline to 19%), a 36% reduction in Cost Per Activation (from $42 to $27), an 18% improvement in 90-Day Retention (71% to 84%), and a 28% increase in Average Revenue Per User ($18/mo to $23/mo).",
        "source_doc": "automotive_new_buyer_campaign.md",
        "category": "performance",
        "route": "ka",
    },
    {
        "qa_id": "QA-008",
        "question": "What was the key insight from the JLR EX90 case study?",
        "expected_answer": "The most important finding was that luxury goods transactional data was the highest-performing single node (importance score 0.38). Geographic clustering around ZIP codes with median household income >$150K improved model precision by 22%. The campaign achieved +217% lift in test drive requests and +325% lift in vehicle configurator completions vs. control.",
        "source_doc": "reference_case_studies.md",
        "category": "case_study",
        "route": "ka",
    },
    {
        "qa_id": "QA-009",
        "question": "What is the pricing for a new Affinity Analysis engagement?",
        "expected_answer": "A new one-time Affinity Analysis has a base fee of $85,000 plus $5,000 per additional segment. An ongoing Standard Affinity Loop costs $45,000 setup plus $12,000/month with volume discounts at 3, 6, and 12-month terms. The new client onboarding fee of $15,000 is waived for contracts over $150K.",
        "source_doc": "new_client_onboarding.md",
        "category": "pricing",
        "route": "ka",
    },
    # Genie-focused (structured data questions)
    {
        "qa_id": "QA-010",
        "question": "How many opportunities are there grouped by tenant name?",
        "expected_answer": "The opportunities table contains opportunities for 5 tenants: AT&T (5 opportunities), JLR (3 opportunities), Pepsi (3 opportunities), Ford (2 opportunities), and Samsung (2 opportunities), for a total of 15 opportunities.",
        "source_doc": "opportunities_table",
        "category": "data_query",
        "route": "genie",
    },
    {
        "qa_id": "QA-011",
        "question": "What is the distribution of opportunity types in the pipeline?",
        "expected_answer": "The opportunity pipeline contains: 5 Existing Affinity Analysis Updates, 6 New Affinity Analyses, and 4 New Client opportunities.",
        "source_doc": "opportunities_table",
        "category": "data_query",
        "route": "genie",
    },
    {
        "qa_id": "QA-012",
        "question": "Which existing opportunities should I review first? Which are incomplete and need attention?",
        "expected_answer": "Four opportunities have null or missing descriptions and are in Draft status, indicating they need attention: OPP-004 (AT&T FirstNet Public Safety), OPP-007 (JLR Defender), OPP-011 (Pepsi Gatorade Youth Athlete), and OPP-013 (Ford Mustang Mach-E EV Consideration). These should be prioritized for description completion before progressing.",
        "source_doc": "opportunities_table",
        "category": "data_query",
        "route": "genie",
    },
    {
        "qa_id": "QA-013",
        "question": "Show me all opportunities with their impact scores and ease scores sorted by impact.",
        "expected_answer": "Ordered by impact score (highest first): Samsung Galaxy S25 — 0.95, AT&T Business Mobility SMB — 0.91, JLR EX90 Conquest — 0.94, AT&T Automotive New Buyer — 0.88, JLR Range Rover Retention — 0.87, JLR Defender — 0.83, AT&T 5G Home Internet — 0.79, Ford Mustang Mach-E — 0.80, AT&T FirstNet — 0.72, Pepsi Next-Gen Sports — 0.76, and others below 0.75.",
        "source_doc": "opportunities_table",
        "category": "data_query",
        "route": "genie",
    },
    {
        "qa_id": "QA-014",
        "question": "What automotive new buyer onboarding campaign data is available and what are the conversion rates by journey stage?",
        "expected_answer": "The AT&T Connected Car automotive new buyer campaign (OPP-001) has data across 5 journey stages: Awareness — Programmatic Display (2.0% conversion), Awareness — CTV/YouTube (4.0% conversion), Consideration — Search (1.6% conversion), Consideration — Social (0.8% conversion), Purchase — Direct Mail (3.0% conversion), and Onboarding — Email (19.0% conversion rate).",
        "source_doc": "campaigns_table",
        "category": "data_query",
        "route": "genie",
    },
    # Mixed (requires both KA and Genie)
    {
        "qa_id": "QA-015",
        "question": "What opportunities do we have for AT&T and what methodology applies to each type?",
        "expected_answer": "AT&T has 5 opportunities: 2 Existing Affinity Analysis Updates (Automotive New Buyer model refresh and Premium TV Bundle loop refresh), 2 New Affinity Analyses (Business Mobility SMB and 5G Home Internet), and 1 Draft (FirstNet). For Existing Updates, the AH refresh process applies updated data vintage; for New Analyses, the full 6-step methodology runs from seed construction through delivery; for the Draft, pre-qualification criteria (50K seed, 12 months data) must first be met.",
        "source_doc": "mixed",
        "category": "mixed",
        "route": "supervisor",
    },
]

with open(SAMPLE_QA_PATH, "w") as f:
    json.dump(SAMPLE_QA, f, indent=2)

print(f"Wrote sample_qa.json: {len(SAMPLE_QA)} Q&A pairs")
by_route = {}
for qa in SAMPLE_QA:
    r = qa.get("route", "unknown")
    by_route[r] = by_route.get(r, 0) + 1
for route, cnt in sorted(by_route.items()):
    print(f"  {route}: {cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write sample Q&A to Unity Catalog table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

qa_schema = StructType([
    StructField("qa_id",           StringType(), False),
    StructField("question",        StringType(), False),
    StructField("expected_answer", StringType(), False),
    StructField("source_doc",      StringType(), False),
    StructField("category",        StringType(), False),
    StructField("route",           StringType(), True),
])

qa_df = spark.createDataFrame(SAMPLE_QA, schema=qa_schema)
qa_df.write.mode("overwrite").saveAsTable(QA_TABLE_BT_FQN)
print(f"Wrote table {QA_TABLE_FQN} ({qa_df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Generate eval_dataset.json (30 examples: 15 hand-crafted + 15 LLM-generated)

# COMMAND ----------

import re

def call_llm(prompt: str, max_tokens: int = 3000) -> str:
    from mlflow.deployments import get_deploy_client
    client = get_deploy_client("databricks")
    response = client.predict(
        endpoint=MODEL,
        inputs={
            "messages": [
                {
                    "role": "system",
                    "content": "You are a Q&A dataset generator for Omnicom Affinity Hub adtech documentation. Output ONLY valid JSON arrays. No markdown fences, no commentary.",
                },
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        },
    )
    return response["choices"][0]["message"]["content"]


def build_generation_prompt(doc_name: str, doc_content: str, count: int, category: str) -> str:
    return f"""Generate exactly {count} realistic Q&A pair(s) about the following Omnicom Affinity Hub document excerpt.

Document: {doc_name}
Category: {category}

Document excerpt:
{doc_content[:2000]}

Each Q&A MUST have these fields:
- qa_id: string (format "EVAL-XXX")
- question: a specific, concrete question an account manager or client might ask (not vague)
- expected_answer: a complete, accurate answer drawn directly from the document
- source_doc: "{doc_name}"
- category: "{category}"
- route: "ka"
- reasoning: 1 sentence explaining why this is a good test question

Create NEW questions different from: {[q['question'] for q in SAMPLE_QA[:5]]}

Output ONLY a JSON array of {count} objects. No other text."""


GENERATION_PLAN = [
    ("att_account_overview.md",        "account_management",    3),
    ("affinity_methodology.md",        "loop_nodes",            3),
    ("automotive_new_buyer_campaign.md","campaign_onboarding",  3),
    ("new_client_onboarding.md",       "contracting",           3),
    ("reference_case_studies.md",      "pepsi_case_study",      3),
]

generated_qas = []
gen_id_counter = 1

for doc_name, category, count in GENERATION_PLAN:
    doc_content = DOCUMENTS[doc_name]
    print(f"Generating {count} Q&A for {doc_name} ({category})...")
    prompt = build_generation_prompt(doc_name, doc_content, count, category)

    try:
        raw = call_llm(prompt)
        raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        raw = re.sub(r"\s*```$", "", raw.strip())
        items = json.loads(raw)
        if not isinstance(items, list):
            items = [items]

        for item in items[:count]:
            item["qa_id"] = f"EVAL-{gen_id_counter:03d}"
            item.setdefault("category", category)
            item.setdefault("source_doc", doc_name)
            item.setdefault("route", "ka")
            generated_qas.append(item)
            gen_id_counter += 1

        print(f"  Generated {len(items[:count])} Q&A pairs.")

    except Exception as e:
        print(f"  WARN: Generation failed for {doc_name}: {e}")
        for i in range(count):
            seed = SAMPLE_QA[i % len(SAMPLE_QA)]
            fallback = {
                "qa_id": f"EVAL-{gen_id_counter:03d}",
                "question": seed["question"] + f" (variant {gen_id_counter})",
                "expected_answer": seed["expected_answer"],
                "source_doc": doc_name,
                "category": category,
                "route": "ka",
            }
            generated_qas.append(fallback)
            gen_id_counter += 1

print(f"\nGenerated {len(generated_qas)} LLM Q&A pairs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b. Validate and combine into eval_dataset

# COMMAND ----------

REQUIRED_FIELDS = {"qa_id", "question", "expected_answer", "source_doc", "category"}

def validate_qa(qa: dict) -> bool:
    if not REQUIRED_FIELDS.issubset(set(qa.keys())):
        return False
    if not isinstance(qa.get("question"), str) or len(qa["question"]) < 10:
        return False
    if not isinstance(qa.get("expected_answer"), str) or len(qa["expected_answer"]) < 10:
        return False
    return True

valid_generated = [qa for qa in generated_qas if validate_qa(qa)]
print(f"Valid generated Q&A: {len(valid_generated)} / {len(generated_qas)}")

eval_dataset_raw = SAMPLE_QA + valid_generated[:15]
print(f"Eval dataset: {len(eval_dataset_raw)} total examples")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8c. Write eval dataset to volume and table

# COMMAND ----------

with open(EVAL_DATASET_PATH, "w") as f:
    json.dump(eval_dataset_raw, f, indent=2)
print(f"Wrote {EVAL_DATASET_PATH}")

eval_df = spark.createDataFrame(eval_dataset_raw)
eval_df.write.mode("overwrite").saveAsTable(EVAL_TABLE_BT_FQN)
print(f"Wrote table {EVAL_TABLE_FQN} ({eval_df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 65)
print("  SETUP COMPLETE — Omnicom Affinity Hub")
print("=" * 65)
print()
print(f"  Catalog: {CATALOG}")
print(f"  Schema:  {CATALOG}.{SCHEMA}")
print(f"  Volume:  {VOLUME_PATH}")
print()
print("  Documents written:")
for fname in DOCUMENTS:
    print(f"    - {DOCS_PATH}/{fname}")
print()
print("  Tables written:")
print(f"    - {OPPORTUNITIES_TABLE_FQN} ({opp_df.count()} rows)")
print(f"    - {CAMPAIGNS_TABLE_FQN} ({cam_df.count()} rows)")
print(f"    - {QA_TABLE_FQN} ({len(SAMPLE_QA)} rows)")
print(f"    - {EVAL_TABLE_FQN} ({eval_df.count()} rows)")
print(f"    - {TENANT_SPS_TABLE_FQN} ({len(tenant_sp_records)} rows)")
print()
print(f"  Genie Space: {GENIE_NAME} (id={GENIE_SPACE_ID})")
print()
print(f"  UC Groups:   {ADMIN_GROUP} + {len(TENANT_GROUPS)} tenant groups")
print(f"  Tenant SPs:  {len(tenant_sp_records)} service principals (one per tenant)")
print(f"  Row Filter:  tenant_row_filter applied to opportunities + campaigns")
print(f"  Col Mask:    mask_budget applied to campaigns.budget (admin-only)")
print()
print("  Files written:")
print(f"    - {SAMPLE_QA_PATH}  ({len(SAMPLE_QA)} Q&A pairs)")
print(f"    - {EVAL_DATASET_PATH}  ({len(eval_dataset_raw)} eval examples)")
print()
print("  Next steps:")
print("    1. Run 02a_setup_agents to create the Knowledge Assistant.")
print("    2. Run 02c_identity to explore identity passthrough patterns.")
print(f"    3. Add GENIE_SPACE_ID={GENIE_SPACE_ID} to databricks.yml / app config.")
print("=" * 65)
