#!/bin/bash
# Runs once when the Postgres data volume is first created.
# Sets up the warehouse database, one schema per layer, and least-privilege roles.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
  CREATE DATABASE ${WAREHOUSE_DB};

  -- loader: used by Airflow ingestion + dbt (read/write)
  CREATE ROLE ${LOADER_USER} LOGIN PASSWORD '${LOADER_PASSWORD}';
  -- metabase_reader: used by the dashboard (read-only, marts only)
  CREATE ROLE ${METABASE_READER_USER} LOGIN PASSWORD '${METABASE_READER_PASSWORD}';
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$WAREHOUSE_DB" <<-SQL
  -- One schema per layer (medallion: staging=silver-in, marts=gold)
  CREATE SCHEMA staging      AUTHORIZATION ${LOADER_USER};
  CREATE SCHEMA intermediate AUTHORIZATION ${LOADER_USER};
  CREATE SCHEMA marts        AUTHORIZATION ${LOADER_USER};

  -- Nobody gets anything by default
  REVOKE ALL ON SCHEMA public FROM PUBLIC;

  -- Reader can only see marts, now and for tables created later by loader
  GRANT USAGE ON SCHEMA marts TO ${METABASE_READER_USER};
  ALTER DEFAULT PRIVILEGES FOR ROLE ${LOADER_USER} IN SCHEMA marts
    GRANT SELECT ON TABLES TO ${METABASE_READER_USER};
SQL

echo "warehouse initialised"
