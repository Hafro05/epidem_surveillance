-- sql/init/01-create-extensions.sql
-- Extensions PostgreSQL utiles pour l'analytics
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Création d'un utilisateur en lecture seule pour Grafana
CREATE USER grafana_reader WITH PASSWORD 'grafana_read_2024!';
GRANT CONNECT ON DATABASE covid_analytics TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_reader;

-- Permissions futures
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
