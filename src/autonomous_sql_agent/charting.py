from __future__ import annotations

from pathlib import Path

import pandas as pd

from autonomous_sql_agent.models import ChartSpec


class ChartService:
    def infer_chart_spec(
        self,
        dataframe: pd.DataFrame,
        chart_hint: str | None = None,
        question: str | None = None,
    ) -> ChartSpec:
        if dataframe.empty:
            return ChartSpec(
                chart_type="table", title="No chart available for empty results"
            )

        numeric_columns = list(dataframe.select_dtypes(include=["number"]).columns)
        object_columns = [
            column for column in dataframe.columns if column not in numeric_columns
        ]
        date_like_columns = [
            column
            for column in dataframe.columns
            if "date" in column.lower()
            or "month" in column.lower()
            or pd.api.types.is_datetime64_any_dtype(dataframe[column])
        ]

        if chart_hint == "line" and date_like_columns and numeric_columns:
            return ChartSpec(
                "line",
                x_field=date_like_columns[0],
                y_field=numeric_columns[0],
                title="Trend Overview",
            )
        if chart_hint == "scatter" and len(numeric_columns) >= 2:
            return ChartSpec(
                "scatter",
                x_field=numeric_columns[0],
                y_field=numeric_columns[1],
                title="Metric Relationship",
            )
        if chart_hint == "bar" and object_columns and numeric_columns:
            return ChartSpec(
                "bar",
                x_field=object_columns[0],
                y_field=numeric_columns[0],
                title="Category Comparison",
            )

        if date_like_columns and numeric_columns:
            return ChartSpec(
                "line",
                x_field=date_like_columns[0],
                y_field=numeric_columns[0],
                title="Trend Overview",
            )
        if object_columns and numeric_columns:
            return ChartSpec(
                "bar",
                x_field=object_columns[0],
                y_field=numeric_columns[0],
                title="Category Comparison",
            )
        if len(numeric_columns) >= 2:
            return ChartSpec(
                "scatter",
                x_field=numeric_columns[0],
                y_field=numeric_columns[1],
                title="Metric Relationship",
            )

        title = "Result Preview"
        if question:
            title = question.strip()[:80]
        return ChartSpec("table", title=title)

    def build_figure(self, dataframe: pd.DataFrame, chart_spec: ChartSpec):
        if dataframe.empty or chart_spec.chart_type == "table":
            return None

        try:
            import plotly.express as px
        except (
            ImportError
        ) as exc:  # pragma: no cover - depends on optional runtime deps
            raise RuntimeError(
                "Plotly is required to render charts in the UI."
            ) from exc

        if chart_spec.chart_type == "line":
            df_plot = dataframe[[chart_spec.x_field, chart_spec.y_field]].copy()
            # Safety net: if each x value is unique AND there are many rows, the SQL
            # returned unaggregated data — collapse to monthly buckets so the trend
            # line is readable instead of a spaghetti mess.
            n_rows = len(df_plot)
            n_unique = df_plot[chart_spec.x_field].nunique()
            if n_rows > 50 and n_unique == n_rows:
                try:
                    df_plot[chart_spec.x_field] = (
                        pd.to_datetime(df_plot[chart_spec.x_field])
                        .dt.to_period("M")
                        .astype(str)
                    )
                    df_plot = df_plot.groupby(chart_spec.x_field, as_index=False)[
                        chart_spec.y_field
                    ].sum()
                except Exception:
                    pass
            # Always sort by x so the line connects points in chronological order
            try:
                df_plot = df_plot.sort_values(chart_spec.x_field).reset_index(drop=True)
            except Exception:
                pass
            return px.line(
                df_plot,
                x=chart_spec.x_field,
                y=chart_spec.y_field,
                title=chart_spec.title,
                markers=True,
            )
        if chart_spec.chart_type == "bar":
            return px.bar(
                dataframe,
                x=chart_spec.x_field,
                y=chart_spec.y_field,
                title=chart_spec.title,
            )
        if chart_spec.chart_type == "scatter":
            return px.scatter(
                dataframe,
                x=chart_spec.x_field,
                y=chart_spec.y_field,
                title=chart_spec.title,
            )
        return None

    def save_chart_image(
        self, dataframe: pd.DataFrame, chart_spec: ChartSpec, output_path: Path
    ) -> Path | None:
        if dataframe.empty or chart_spec.chart_type == "table":
            return None

        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:  # pragma: no cover - depends on optional runtime deps
            return None

        output_path.parent.mkdir(parents=True, exist_ok=True)
        subset = dataframe.head(20)
        plt.figure(figsize=(9, 4.5))
        if chart_spec.chart_type == "line":
            plt.plot(subset[chart_spec.x_field], subset[chart_spec.y_field], marker="o")
        elif chart_spec.chart_type == "bar":
            plt.bar(subset[chart_spec.x_field].astype(str), subset[chart_spec.y_field])
            plt.xticks(rotation=30, ha="right")
        elif chart_spec.chart_type == "scatter":
            plt.scatter(subset[chart_spec.x_field], subset[chart_spec.y_field])
        else:
            plt.close()
            return None

        plt.title(chart_spec.title or "Analysis Chart")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()
        return output_path
