-- Core Banking Fraud Detection — PGD Init

-- For demonstration purposes only.

-- Database 'demo' is created by PGD entrypoint
-- User 'postgres' is the default PGD superuser

-- Enable extensions (graceful — some may not be installed)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgfs CASCADE;
CREATE EXTENSION IF NOT EXISTS pgaa CASCADE;
CREATE EXTENSION IF NOT EXISTS aidb CASCADE;

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;

-- Grant replication role
ALTER USER postgres WITH REPLICATION;

-- Lakekeeper catalog database (instead of separate container)
-- Idempotent: only create if not exists
SELECT 'CREATE DATABASE lakekeeper'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'lakekeeper')\gexec

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lakekeeper') THEN
    CREATE USER lakekeeper WITH PASSWORD 'lakekeeper';
  END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE lakekeeper TO lakekeeper;
ALTER DATABASE lakekeeper OWNER TO lakekeeper;

-- Connect to lakekeeper db and grant schema permissions
\c lakekeeper
GRANT ALL ON SCHEMA public TO lakekeeper;
\c demo

-- LangFlow database for AI agent builder
-- Idempotent: only create if not exists
SELECT 'CREATE DATABASE langflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'langflow')\gexec

GRANT ALL PRIVILEGES ON DATABASE langflow TO postgres;
