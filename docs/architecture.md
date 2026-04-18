# Architecture Overview

`User Question -> IntentAgent -> PlanningAgent -> SchemaAgent -> SQLGenerationAgent -> SQLValidationAgent -> ExecutionAgent -> PatternDiscoveryAgent -> ReportAgent -> ExportService`

## Core Components

- `DatabaseManager`: PostgreSQL access, schema metadata, query execution, and session logging
- `SchemaMetadataService`: compact schema grounding and business glossary
- `HuggingFaceSQLGenerator`: primary NL-to-SQL generation with a fallback template generator
- `AnalyticsService`: trend and anomaly summaries for result frames
- `ChartService`: auto-selects one minimal chart and can render an image for PDF export
- `AnalysisOrchestrator`: coordinates the multi-agent pipeline end to end

## Warehouse Domain

The retail/e-commerce warehouse uses a star schema with:

- Dimensions: `dim_date`, `dim_region`, `dim_channel`, `dim_customer`, `dim_product`
- Facts: `fact_orders`, `fact_order_items`, `fact_returns`
- App metadata: `app_analysis_sessions`
