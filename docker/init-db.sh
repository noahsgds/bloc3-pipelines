#!/bin/sh
# Exécuté une seule fois, à la création de la base Postgres.
# - base "airflow" : état interne d'Airflow (créée par POSTGRES_DB) ;
# - base "pipeline" : les données du pipeline ;
# - rôle "lecture_seule" : accès en lecture pour la supervision (Grafana, bloc 2),
#   sans droit de modification (moindre privilège).
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<SQL
CREATE DATABASE pipeline;
CREATE ROLE lecture_seule LOGIN PASSWORD '${READONLY_PASSWORD}';
\c pipeline
CREATE SCHEMA IF NOT EXISTS pipeline;
GRANT USAGE ON SCHEMA pipeline TO lecture_seule;
ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT SELECT ON TABLES TO lecture_seule;
SQL
