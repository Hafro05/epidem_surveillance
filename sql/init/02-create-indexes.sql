-- sql/init/02-create-indexes.sql
-- Index optimisés pour les requêtes Grafana
-- Ces index seront créés après les tables par l'ETL

-- Index pour les requêtes temporelles
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_covid_date_country ON covid_daily_data (date, iso_code);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_covid_incidence_date ON covid_daily_data (incidence_rate_100k, date) WHERE incidence_rate_100k IS NOT NULL;

-- Index pour les agrégations
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_covid_vaccination ON covid_daily_data (vaccination_rate, date) WHERE vaccination_rate IS NOT NULL;

-- Index pour les alertes
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_alerts_active_type ON covid_alerts (is_active, alert_type, alert_date);

-- Statistiques pour l'optimiseur
-- ANALYZE;