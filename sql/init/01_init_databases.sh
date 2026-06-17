#!/bin/bash
# ============================================================
# DK-DE-OnPrem-CustomPlatform
# Script d'initialisation PostgreSQL
# Fichier : sql/init/01_init_databases.sh
# Exécuté automatiquement au premier démarrage du conteneur
#
# Les identifiants sont lus depuis les variables d'environnement
# (transmises par docker-compose via le fichier .env) :
#   POSTGRES_USER / POSTGRES_DB : compte interne (metadata)
#   DW_USER / DW_PASSWORD / DW_DBNAME : Data Warehouse
# Aucun secret n'est codé en dur.
# ============================================================
set -e

# ------------------------------------------------------------
# Création de l'utilisateur Data Warehouse (idempotent)
# ------------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
       IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DW_USER}') THEN
          CREATE USER ${DW_USER} WITH PASSWORD '${DW_PASSWORD}';
       END IF;
    END
    \$\$;
EOSQL

# ------------------------------------------------------------
# Création de la base Data Warehouse si elle n'existe pas
# ------------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tc \
    "SELECT 1 FROM pg_database WHERE datname = '${DW_DBNAME}'" | grep -q 1 || \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "CREATE DATABASE ${DW_DBNAME} OWNER ${DW_USER};"

# ------------------------------------------------------------
# Droits sur la base
# ------------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "GRANT ALL PRIVILEGES ON DATABASE ${DW_DBNAME} TO ${DW_USER};"

# ------------------------------------------------------------
# Création des schémas dans la base Data Warehouse
# ------------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${DW_DBNAME}" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION ${DW_USER};
    CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION ${DW_USER};
    CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION ${DW_USER};
    CREATE SCHEMA IF NOT EXISTS audit  AUTHORIZATION ${DW_USER};

    COMMENT ON SCHEMA bronze IS 'Donnees brutes ingerees : aucune transformation';
    COMMENT ON SCHEMA silver IS 'Donnees nettoyees et normalisees : dbt models';
    COMMENT ON SCHEMA gold   IS 'Tables analytiques : Faits et Dimensions';
    COMMENT ON SCHEMA audit  IS 'Logs pipeline, erreurs, historique executions';
EOSQL

echo "Compte ${DW_USER}, base ${DW_DBNAME} et schemas (bronze/silver/gold/audit) initialises"