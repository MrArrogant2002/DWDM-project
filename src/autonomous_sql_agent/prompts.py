from __future__ import annotations

from textwrap import dedent

# ---------------------------------------------------------------------------
# SQL generation prompt — used by the HF Inference API call
# ---------------------------------------------------------------------------

SQL_SYSTEM_PROMPT = (
    "You are an expert SQL analyst. "
    "You write correct SQLite SELECT queries and return structured JSON. "
    "Never include explanatory text outside the JSON block."
)

SQL_USER_TEMPLATE = dedent(
    """
    Database schema (SQLite 3):
    {schema_context}

    User question: {question}

    Analysis hints:
    {analysis_plan}

    SQLite-only syntax rules (strictly enforced):
    - Use exact table and column names from the schema — no aliases for table names.
    - Single SELECT statement only. No INSERT / UPDATE / DELETE / DDL.
    - Dates: use  strftime('%Y', col)  strftime('%m', col)  strftime('%Y-%m', col)
      NEVER use EXTRACT(), DATE_TRUNC(), or TO_DATE().
    - Type cast: use  CAST(x AS INTEGER)  not  x::integer  or  x::text.
    - String comparison: use  LIKE  (SQLite LIKE is case-insensitive) — no ILIKE.
    - No window functions (ROW_NUMBER OVER, RANK OVER) unless column exists.
    - Use LIMIT only when ranking or top-N output is requested.
    - Set "needs_summary" to true ONLY when the user explicitly asks for a
      summary, explanation, or interpretation of the results.

    Respond with ONLY valid JSON — no markdown fences, no extra text:
    {{
      "plan": ["step 1", "step 2"],
      "sql": "SELECT ...",
      "analysis_goal": "one-sentence description",
      "tables_used": ["table_name"],
      "chart_hint": "line|bar|scatter|table",
      "needs_summary": false
    }}
    """
).strip()

# ---------------------------------------------------------------------------
# Summary prompt — used only when needs_summary is true
# ---------------------------------------------------------------------------

SUMMARY_USER_TEMPLATE = dedent(
    """
    The user asked: "{question}"

    The SQL query returned {row_count} rows. Here is a preview:
    {data_preview}

    Write a concise 3-5 sentence plain-English summary of the findings.
    Focus on the most important insight and any notable pattern.
    """
).strip()


def build_sql_messages(
    question: str,
    analysis_plan: list[str],
    schema_context: str,
) -> list[dict[str, str]]:
    plan_text = (
        "\n".join(f"- {s}" for s in analysis_plan)
        or "- Answer the question accurately."
    )
    return [
        {"role": "system", "content": SQL_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": SQL_USER_TEMPLATE.format(
                schema_context=schema_context.strip(),
                question=question.strip(),
                analysis_plan=plan_text,
            ),
        },
    ]


def build_summary_messages(
    question: str,
    data_preview: str,
    row_count: int,
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": "You are a helpful data analyst who summarizes query results clearly.",
        },
        {
            "role": "user",
            "content": SUMMARY_USER_TEMPLATE.format(
                question=question.strip(),
                row_count=row_count,
                data_preview=data_preview,
            ),
        },
    ]


# Kept for backward-compat with tests that import build_sql_prompt
def build_sql_prompt(
    question: str,
    analysis_plan: list[str],
    schema_context: str,
    glossary: dict[str, str],
) -> str:
    plan_text = "\n".join(f"- {s}" for s in analysis_plan)
    glossary_text = "\n".join(f"- {k}: {v}" for k, v in glossary.items())
    return f"Question: {question}\nPlan:\n{plan_text}\nSchema:\n{schema_context}\nGlossary:\n{glossary_text}"
