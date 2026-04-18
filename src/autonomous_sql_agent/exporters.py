from __future__ import annotations

from datetime import datetime
from pathlib import Path
from re import sub

import pandas as pd

from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.logging_utils import get_logger
from autonomous_sql_agent.models import ChartSpec, DownloadArtifacts

logger = get_logger(__name__)


class ExportService:
    def __init__(self, config: AppConfig, chart_service: ChartService) -> None:
        self.config = config
        self.chart_service = chart_service
        self.config.export_dir.mkdir(parents=True, exist_ok=True)

    def export_analysis(
        self,
        question: str,
        approved_sql: str,
        dataframe: pd.DataFrame,
        insights: list[str],
        chart_spec: ChartSpec | None,
    ) -> DownloadArtifacts:
        slug = self._slugify(question)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = f"{timestamp}_{slug}"

        csv_path = self.config.export_dir / f"{stem}.csv"
        xlsx_path = self.config.export_dir / f"{stem}.xlsx"
        pdf_path = self.config.export_dir / f"{stem}.pdf"

        artifacts = DownloadArtifacts()
        try:
            dataframe.to_csv(csv_path, index=False)
            artifacts.csv_path = str(csv_path)
        except Exception as exc:
            logger.warning("CSV export failed: %s", exc)

        try:
            self._write_excel(xlsx_path, dataframe, insights, approved_sql)
            artifacts.xlsx_path = str(xlsx_path)
        except Exception as exc:
            logger.warning("Excel export failed: %s", exc)

        try:
            self._write_pdf(pdf_path, question, approved_sql, dataframe, insights, chart_spec, stem)
            artifacts.pdf_path = str(pdf_path)
        except Exception as exc:
            logger.warning("PDF export failed: %s", exc)

        return artifacts

    @staticmethod
    def _slugify(text: str) -> str:
        cleaned = sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
        return cleaned[:50] or "analysis"

    @staticmethod
    def _write_excel(path: Path, dataframe: pd.DataFrame, insights: list[str], approved_sql: str) -> None:
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            dataframe.to_excel(writer, sheet_name="results", index=False)
            summary = pd.DataFrame(
                {
                    "section": ["sql", "insight_1", "insight_2", "insight_3"],
                    "content": [approved_sql] + insights[:3] + [""] * max(0, 3 - len(insights)),
                }
            )
            summary.to_excel(writer, sheet_name="summary", index=False)

    def _write_pdf(
        self,
        path: Path,
        question: str,
        approved_sql: str,
        dataframe: pd.DataFrame,
        insights: list[str],
        chart_spec: ChartSpec | None,
        stem: str,
    ) -> None:
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
        except ImportError as exc:  # pragma: no cover - depends on optional runtime deps
            raise RuntimeError("reportlab is required to generate PDF exports.") from exc

        chart_image = None
        if chart_spec is not None:
            chart_image = self.chart_service.save_chart_image(
                dataframe,
                chart_spec,
                self.config.export_dir / f"{stem}.png",
            )

        pdf = canvas.Canvas(str(path), pagesize=A4)
        width, height = A4
        y = height - 50
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(40, y, "Autonomous SQL Agent Report")
        y -= 24

        pdf.setFont("Helvetica", 10)
        pdf.drawString(40, y, f"Generated: {datetime.now().isoformat(timespec='seconds')}")
        y -= 18
        pdf.drawString(40, y, f"Question: {question[:100]}")
        y -= 24

        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(40, y, "Insights")
        y -= 16
        pdf.setFont("Helvetica", 10)
        for insight in insights[:3]:
            pdf.drawString(48, y, f"- {insight[:105]}")
            y -= 14

        y -= 8
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(40, y, "SQL")
        y -= 16
        pdf.setFont("Helvetica", 8)
        for line in approved_sql.strip().splitlines()[:14]:
            pdf.drawString(48, y, line[:118])
            y -= 11

        if chart_image is not None and y > 220:
            y -= 10
            pdf.drawImage(str(chart_image), 40, y - 180, width=500, height=180, preserveAspectRatio=True)
            y -= 190

        y -= 8
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(40, y, "Preview Rows")
        y -= 16
        pdf.setFont("Helvetica", 8)
        preview = dataframe.head(6).fillna("").astype(str)
        for row in preview.to_dict("records"):
            pdf.drawString(48, y, str(row)[:120])
            y -= 10
            if y < 60:
                pdf.showPage()
                y = height - 50
                pdf.setFont("Helvetica", 8)

        pdf.save()
