from __future__ import annotations

from statistics import mean, pstdev

import pandas as pd


class AnalyticsService:
    def analyze(self, dataframe: pd.DataFrame, intent: str | None) -> tuple[list[str], list[str]]:
        if dataframe.empty:
            return (
                ["The query completed successfully, but there were no matching records in the warehouse."],
                ["Try widening the time range or removing one filter to discover more rows."],
            )

        insights = [self._summarize_primary_signal(dataframe)]
        anomaly_insight = self._detect_anomaly(dataframe)
        if anomaly_insight:
            insights.append(anomaly_insight)

        trend_insight = self._detect_trend(dataframe)
        if trend_insight:
            insights.append(trend_insight)

        if intent == "segmentation":
            cluster_insight = self._cluster_hint(dataframe)
            if cluster_insight:
                insights.append(cluster_insight)

        follow_ups = self._recommended_followups(intent)
        return insights, follow_ups

    @staticmethod
    def _summarize_primary_signal(dataframe: pd.DataFrame) -> str:
        numeric_columns = list(dataframe.select_dtypes(include=["number"]).columns)
        if not numeric_columns:
            return f"The result returned {len(dataframe)} records without a dominant numeric KPI."

        target_metric = numeric_columns[0]
        if len(dataframe) == 1:
            value = dataframe.iloc[0][target_metric]
            return f"The primary metric `{target_metric}` equals `{value}` for the selected slice."

        label_column = next((column for column in dataframe.columns if column != target_metric), None)
        top_row = dataframe.sort_values(by=target_metric, ascending=False).iloc[0]
        if label_column:
            return (
                f"The strongest result is `{top_row[label_column]}` with `{target_metric}` = "
                f"`{round(float(top_row[target_metric]), 2)}`."
            )
        return f"The highest observed `{target_metric}` is `{round(float(top_row[target_metric]), 2)}`."

    @staticmethod
    def _detect_anomaly(dataframe: pd.DataFrame) -> str | None:
        numeric_columns = list(dataframe.select_dtypes(include=["number"]).columns)
        if not numeric_columns or len(dataframe) < 5:
            return None

        target_metric = numeric_columns[0]
        values = dataframe[target_metric].fillna(0).astype(float).tolist()
        deviation = pstdev(values)
        if deviation == 0:
            return None

        midpoint = mean(values)
        z_scores = [(value - midpoint) / deviation for value in values]
        max_index = max(range(len(z_scores)), key=lambda index: abs(z_scores[index]))
        if abs(z_scores[max_index]) < 1.75:
            return None

        label_column = next((column for column in dataframe.columns if column != target_metric), None)
        row = dataframe.iloc[max_index]
        label = row[label_column] if label_column else f"row {max_index + 1}"
        return (
            f"Potential anomaly detected: `{label}` deviates sharply on `{target_metric}` "
            f"with a z-score of `{round(z_scores[max_index], 2)}`."
        )

    @staticmethod
    def _detect_trend(dataframe: pd.DataFrame) -> str | None:
        numeric_columns = list(dataframe.select_dtypes(include=["number"]).columns)
        date_like_columns = [
            column
            for column in dataframe.columns
            if "date" in column.lower() or "month" in column.lower() or pd.api.types.is_datetime64_any_dtype(dataframe[column])
        ]
        if not numeric_columns or not date_like_columns or len(dataframe) < 3:
            return None

        target_metric = numeric_columns[0]
        frame = dataframe.copy()
        frame = frame.sort_values(by=date_like_columns[0])
        latest_value = float(frame.iloc[-1][target_metric])
        baseline_value = float(frame.iloc[0][target_metric])
        if baseline_value == 0:
            return None

        delta_pct = ((latest_value - baseline_value) / baseline_value) * 100
        direction = "upward" if delta_pct >= 0 else "downward"
        return f"The time series shows an overall {direction} movement of `{round(delta_pct, 2)}%` from first to latest period."

    @staticmethod
    def _cluster_hint(dataframe: pd.DataFrame) -> str | None:
        numeric_columns = list(dataframe.select_dtypes(include=["number"]).columns)
        if len(numeric_columns) < 2 or len(dataframe) < 12:
            return None
        return (
            "This result has enough numeric density to support customer or region clustering in a follow-up analysis."
        )

    @staticmethod
    def _recommended_followups(intent: str | None) -> list[str]:
        if intent == "anomaly":
            return [
                "Break the anomaly down by region or channel to locate the main driver.",
                "Compare the same metric against the previous month or quarter.",
            ]
        if intent == "trend":
            return [
                "Split the trend by product category or channel to explain the movement.",
                "Add return rate alongside sales to check whether growth came with quality issues.",
            ]
        if intent == "segmentation":
            return [
                "Compare each segment by revenue and return rate.",
                "Check whether one loyalty tier is over-represented inside the strongest cluster.",
            ]
        return [
            "Slice the result by month, region, or product category for more detail.",
            "Run a follow-up anomaly scan on the top-performing rows.",
        ]
