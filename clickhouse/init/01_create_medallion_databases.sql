-- Medallion layers: one ClickHouse database per layer.
--   bronze : raw data exactly as it was ingested (ELT: load first, transform later)
--   silver : cleaned and conformed data
--   gold   : business-ready aggregates
--
-- This file runs automatically the FIRST time the ClickHouse container starts
-- with an empty data volume. It is safe to run again by hand (IF NOT EXISTS).
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
