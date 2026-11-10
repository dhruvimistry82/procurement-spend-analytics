-- Run once in a Databricks SQL editor or notebook.
-- If your workspace doesn't allow creating catalogs, replace `procurement` with `workspace`
-- here, in config/sources.yml (volume_dir) and in the pipeline's landing_path setting.
CREATE CATALOG IF NOT EXISTS procurement;
CREATE SCHEMA  IF NOT EXISTS procurement.raw;
CREATE SCHEMA  IF NOT EXISTS procurement.analytics;   -- pipeline target (bronze/silver/gold tables)
CREATE VOLUME  IF NOT EXISTS procurement.raw.landing;
CREATE VOLUME  IF NOT EXISTS procurement.raw.landing_synthetic;  -- synthetic test data
