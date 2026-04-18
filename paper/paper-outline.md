# AutSQL — IEEE Paper Outline
**Title:** AutSQL: An Autonomous Multi-Agent Framework for Natural Language Querying of Retail Data Warehouses
**Target Venue:** IEEE ICDM 2025 / DASFAA 2025 (conference, 10 pages, IEEEtran)
**Template:** Systems / Applied paper
**Files:** `paper.tex`, `references.bib`, `figures/` (placeholders)

---

## Abstract (150–250 words)
**Structure:** context → problem → method → results (with numbers) → significance

| Beat | Content | Status |
|------|---------|--------|
| Context | Retail star-schema warehouses inaccessible to non-SQL users | Draft in .tex |
| Problem | NL2SQL systems miss safety + downstream analytics | Draft in .tex |
| Method | 8-agent AutSQL pipeline overview | Draft in .tex |
| Results | EA=TODO%, RC=TODO%, SCR=100%, latency=TODO s | **Fill after experiments** |
| Significance | Narrows gap between warehouse data and business insight | Draft in .tex |

---

## Section 1 — Introduction
**Purpose:** Motivate, identify gap, state contributions verbatim, give roadmap.

| Beat | Key point | Citations needed |
|------|-----------|-----------------|
| 1 | Data-driven retail; warehouse data inaccessible without SQL | `\cite{data-democratisation}` |
| 2 | NL2SQL systems are generation-only; no safety, no analytics | `\cite{TODO:seq2sql,picard,dinsql}` Spider `\cite{spider}` |
| 3 | Introduce AutSQL as a multi-agent solution | — |
| 4 | **5 contribution bullets (verbatim — already in paper.tex)** | `\cite{sqlcoder,isolation-forest}` |
| 5 | Paper roadmap | — |

**Figure needed:** None in Introduction.

---

## Section 2 — Related Work
**Purpose:** Group prior work by family; end each paragraph with the gap that motivates AutSQL.

### 2.1 NL-to-SQL with Neural Models
- Sequence-to-sequence era: Seq2SQL, SQLNet, RAT-SQL, BRIDGE
- LLM era: SQLCoder, DIN-SQL, GPT-4 baselines, Spider leaderboard
- **Gap:** generation-only; no safety enforcement or result interpretation

### 2.2 Autonomous Agent Frameworks
- ReAct, Toolformer, AutoGPT
- Data science agents: DS-Agent, Data Copilot
- **Gap:** not warehouse-specific; no AST-level SQL safety

### 2.3 Conversational Warehouse Interfaces
- Conversational OLAP, NL4DV
- **Gap:** symbolic/grammar-based; limited expressivity on ad-hoc retail questions

| Citations needed |
|-----------------|
| `\cite{seq2sql,sqlnet,ratsql,bridge,spider,picard,dinsql}` |
| `\cite{sqlcoder,gpt4,codellama,nl2sql-survey,nl2sql-leaderboard}` |
| `\cite{react,toolformer,autogpt,multiagent-survey,datacopilot}` |
| `\cite{convOLAP,nl4dv,atis}` |

---

## Section 3 — System Design
**Purpose:** Present architecture: 8 agents, AgentState, orchestrator, safety model, star schema.

### 3.1 Architecture Overview
- Agent pipeline: `IntentAgent → PlanningAgent → SchemaAgent → SQLGenerationAgent ⇄ SQLValidationAgent ⇄ ExecutionAgent → PatternDiscoveryAgent → ReportAgent`
- Shared `AgentState` Pydantic dataclass as the coordination primitive
- **Figure 1:** Agent pipeline flow diagram (directed, colour-coded)

### 3.2 Agent Roles and Responsibilities
- Table 1: Agent × Input × Transformation × Output
- Intent categories: anomaly, trend, segmentation, comparison, summary
- Schema grounding: PostgreSQL catalogue introspection + business glossary

### 3.3 SQL Generation and Safety Model
- SQLCoder-7B-2 in 4-bit quantised mode (BitsAndBytes)
- Deterministic rule-based fallback for 5 template categories
- **Figure 2:** Two-stage safety gate (sqlglot AST → EXPLAIN → execute)
- Retry loop: up to `MAX_GENERATION_RETRIES` with `error_feedback`

### 3.4 Warehouse Schema
- Kimball star schema: 3 fact tables + 5 dimension tables
- Explicit KPIs: revenue, discount, return amount, return reason, shipping time, order status
- **Figure 3:** ER diagram of the star schema

| Citations needed |
|-----------------|
| `\cite{multiagent-survey}` for pipeline multi-agent architecture |
| `\cite{sqlcoder,qlora,bitsandbytes}` for model loading |
| `\cite{sqlglot}` for AST-based validation |
| `\cite{kimball-dw}` for star schema |

---

## Section 4 — Implementation
**Purpose:** Describe technical choices: model, analytics algorithms, UI, exports, reproducibility.

### 4.1 NL-to-SQL Model Integration
- 4-bit NF quantisation with `BitsAndBytesConfig`; memory: ~14 GB fp16 → ~4.5 GB int4
- Prompt template: 6 components (question, schema, glossary, plan, rules, JSON output spec)
- Deterministic decoding: `do_sample=False`
- Citations: `\cite{sqlcoder,qlora,bitsandbytes,transformers}`

### 4.2 Adaptive Analytics Module
- **Anomaly:** z-score (`|z| ≥ 1.75`) for univariate; IsolationForest (`n_estimators=100`, `contamination=0.1`) for multivariate
- **Trend:** 3-period SMA; directional change % from first to last period
- **Segmentation hint:** flag for K-Means when ≥ 2 numeric cols, ≥ 12 rows
- Citations: `\cite{isolation-forest}`

### 4.3 Chart Selection
- Decision rules: time series → line; categorical×numeric → bar; 2 numeric → scatter; else table
- Plotly rendering; `ChartSpec` Pydantic serialisation
- Citations: `\cite{plotly}`

### 4.4 Export and Session Logging
- CSV: raw result only
- XLSX: result + insight summary sheet
- PDF: question + SQL + insights + chart image + timestamp (reportlab)
- `session_history` table: session_id, question, approved_sql, row_count, chart_type, paths, warnings, created_at
- Citations: `\cite{reportlab}`

### 4.5 Streamlit User Interface
- Main panel: question box, sample prompts, plan summary, SQL display, validation warnings, result preview (200 rows), insight text, chart, downloads
- Sidebar: schema snapshot, session history
- Citations: `\cite{streamlit}`

---

## Section 5 — Experiments and Evaluation
**Purpose:** Rigorous experimental setup + results for each research question.

### 5.1 Experimental Setup
- **Dataset:** 100 K orders, ~500 K line items, 20 K customers, 500 products, 5 regions (synthetic, Faker seed=42)
- **Benchmark:** TODO-question set covering: top products, monthly trend, return anomalies, customer segmentation, comparison queries, zero-result queries, multi-join queries
- **Baselines:** (1) rule-based fallback only; (2) SQLCoder-7B-2 without AutSQL pipeline; (3) TODO (GPT-4?)
- **Metrics:** EA (execution accuracy), RC (result correctness), SCR (safety compliance rate), Latency P50/P95
- **Hardware:** NVIDIA TODO GPU, TODO GB RAM; seeds: numpy=42, random=42, sklearn random_state=42

### 5.2 Main Results
- Table 2: EA / RC / SCR / Latency per system
- Table 3: Anomaly detection Precision / Recall / F1 (z-score vs. IsolationForest vs. Combined)

### 5.3 Ablation Study
- Table 4: Full pipeline vs. –safety layer vs. –retry loop vs. –schema grounding vs. –analytics module

### 5.4 Qualitative Analysis
- Example walkthrough: "Which regions showed unusual return spikes last quarter?"
- **Figure 4:** Annotated Streamlit UI screenshot

---

## Section 6 — Discussion
**Purpose:** Interpret results, state limitations and threats to validity.

| Beat | Content |
|------|---------|
| Safety guarantees | 100% SCR on evaluated inputs; discuss adversarial limits |
| Generation quality | EA vs. RC gap attribution (schema grounding errors, hallucination) |
| Limitations (5 items) | Synthetic data, English-only, single-user, model latency, heuristic intent classifier |
| Threats to validity | Author-designed ground-truth SQL; PostgreSQL-only |

---

## Section 7 — Conclusion
**Purpose:** Restate contributions, summarise evidence, future work.

| Beat | Content |
|------|---------|
| Restate | 8-agent pipeline + 100% SCR + adaptive analytics |
| Evidence | EA=TODO%, RC=TODO%, ablation gains |
| Future work | LLM-based IntentAgent, multi-turn safety, real-world validation, feedback loop |

---

## Figures and Tables Checklist

| # | Type | File | Status |
|---|------|------|--------|
| Fig 1 | Agent pipeline flow diagram | `figures/pipeline.pdf` | TODO |
| Fig 2 | Safety gate flowchart | `figures/safety_gate.pdf` | TODO |
| Fig 3 | Star schema ER diagram | `figures/schema.pdf` | TODO |
| Fig 4 | Streamlit UI screenshot | `figures/ui_screenshot.pdf` | TODO |
| Table 1 | Agent roles | In paper.tex | Draft ✓ |
| Table 2 | Main results | In paper.tex | Fill after experiments |
| Table 3 | Anomaly detection results | In paper.tex | Fill after experiments |
| Table 4 | Ablation study | In paper.tex | Fill after experiments |

---

## Citation TODO List (high priority — fill before submission)

| Key | What to find |
|-----|-------------|
| `data-democratisation` | Self-service BI / data democratisation academic or industry source |
| `nl2sql-survey` | Qin et al. 2022 arXiv:2208.13629 — verify venue |
| `convOLAP` | Primary conversational OLAP paper |
| `datacopilot` | Data Copilot or equivalent LLM data-science agent paper |
| `retail-kpi` | Published retail KPI taxonomy or data modelling reference |
| `sql-gen-reproducibility` | LLM evaluation / deterministic decoding reproducibility paper |
| `nl2sql-leaderboard` | Yale LILY Spider leaderboard — update access date |
| All `TODO: update with proceedings` keys | Replace arXiv preprints with published proceedings once confirmed |

---

## Next Steps (priority order for tomorrow deadline)

1. **Run experiments** — seed the warehouse, build the benchmark question set, measure EA/RC/SCR/latency.
2. **Fill Table 2, 3, 4** with measured numbers.
3. **Write Abstract** — replace placeholder with 150–250 words including measured numbers.
4. **Run `/paper-draft introduction`** to fill Section 1 prose.
5. **Run `/paper-draft related-work`** to fill Section 2.
6. **Run `/paper-draft method`** to fill Sections 3–4.
7. **Run `/paper-draft experiments`** to fill Section 5.
8. **Create figures** — pipeline diagram, safety gate, ER diagram, UI screenshot.
9. **Resolve citation TODOs** — use `/cite` skill for each `TODO:` key.
10. **Run `/paper-polish`** for a final prose pass before submission.
