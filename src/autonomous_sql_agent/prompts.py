from __future__ import annotations

from textwrap import dedent


SQL_PROMPT_TEMPLATE = dedent(
    """
    You are an expert SQL generation agent for a retail and e-commerce data warehouse.
    Produce a single PostgreSQL SELECT query that answers the business question.

    Business question:
    {question}

    Analysis plan:
    {analysis_plan}

    Schema context:
    {schema_context}

    Business glossary:
    {glossary}

    SQL rules:
    - Return one SELECT statement only.
    - Use explicit JOIN clauses.
    - Prefer business-friendly aliases.
    - Use fact_orders.total_amount for sales totals unless item level detail is required.
    - Use fact_returns.return_amount for return-value analysis.
    - Use dim_date for month, quarter, and time-based grouping.
    - Avoid SELECT *.
    - Include LIMIT only when ranking output is clearly requested.
    - Never generate INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, or multi-statement SQL.

    Return a JSON object with this shape:
    {{
      "sql": "<postgresql query>",
      "analysis_goal": "<short sentence>",
      "tables_used": ["table_a", "table_b"],
      "chart_hint": "line|bar|scatter|table"
    }}
    """
).strip()


def build_sql_prompt(
    question: str,
    analysis_plan: list[str],
    schema_context: str,
    glossary: dict[str, str],
) -> str:
    plan_text = "\n".join(f"- {item}" for item in analysis_plan) or "- Answer the user's question accurately."
    glossary_text = "\n".join(f"- {term}: {meaning}" for term, meaning in glossary.items())
    return SQL_PROMPT_TEMPLATE.format(
        question=question.strip(),
        analysis_plan=plan_text,
        schema_context=schema_context.strip(),
        glossary=glossary_text,
    )
