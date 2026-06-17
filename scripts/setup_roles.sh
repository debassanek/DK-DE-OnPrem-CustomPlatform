#!/bin/bash
# =================================================================
# DK-DE-OnPrem-CustomPlatform
# Script : setup_roles.sh
# Description : Applique la couche de gouvernance (rôles, permissions,
#               RLS, vues sécurisées) sur la base Data Warehouse.
#               Les mots de passe des comptes techniques sont lus
#               depuis les variables d'environnement (.env).
#
# Usage : bash scripts/setup_roles.sh
# Prérequis : variables AIRFLOW_PASSWORD, PBI_SERVICE_PASSWORD,
#             MONITORING_PASSWORD, DATA_ACCESS_PASSWORD définies
# =================================================================
set -e

# ----------------------------------------------------------------
# Configuration de connexion
# ----------------------------------------------------------------
DB_CONTAINER="dkde_postgres"
DB_SUPERUSER="${POSTGRES_USER:-airflow}"
DB_NAME="${DW_DBNAME:-customplatform}"
SQL_DIR="/docker-entrypoint-initdb.d"

echo "=== Application de la couche de gouvernance ==="
echo "Base : ${DB_NAME}"

# ----------------------------------------------------------------
# 03 : Rôles et comptes techniques (avec mots de passe injectés)
# ----------------------------------------------------------------
echo ">> 03 : création des rôles et comptes..."
docker exec -i "${DB_CONTAINER}" psql \
    -v ON_ERROR_STOP=1 \
    -v AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD}" \
    -v PBI_SERVICE_PASSWORD="${PBI_SERVICE_PASSWORD}" \
    -v MONITORING_PASSWORD="${MONITORING_PASSWORD}" \
    -v DATA_ACCESS_PASSWORD="${DATA_ACCESS_PASSWORD}" \
    -U "${DB_SUPERUSER}" -d "${DB_NAME}" \
    -f "${SQL_DIR}/03_roles_securite.sql"

# ----------------------------------------------------------------
# 04 : Permissions sur les schémas
# ----------------------------------------------------------------
echo ">> 04 : attribution des permissions..."
docker exec -i "${DB_CONTAINER}" psql \
    -v ON_ERROR_STOP=1 \
    -U "${DB_SUPERUSER}" -d "${DB_NAME}" \
    -f "${SQL_DIR}/04_permissions.sql"

# ----------------------------------------------------------------
# 05 : Row Level Security
# ----------------------------------------------------------------
echo ">> 05 : activation de la RLS..."
docker exec -i "${DB_CONTAINER}" psql \
    -v ON_ERROR_STOP=1 \
    -U "${DB_SUPERUSER}" -d "${DB_NAME}" \
    -f "${SQL_DIR}/05_rls.sql"

# ----------------------------------------------------------------
# 06 : Vues sécurisées
# ----------------------------------------------------------------
echo ">> 06 : création des vues sécurisées..."
docker exec -i "${DB_CONTAINER}" psql \
    -v ON_ERROR_STOP=1 \
    -U "${DB_SUPERUSER}" -d "${DB_NAME}" \
    -f "${SQL_DIR}/06_vues_securisees.sql"

echo ""
echo "=== Gouvernance appliquée avec succès ==="
echo "  - Rôles groupes : role_pipeline, role_powerbi, role_data"
echo "  - Comptes : user_airflow, user_pbi_service, user_monitoring, user_data"
echo "  - RLS active sur les 4 tables de faits Gold"
echo "  - Vue sécurisée : gold.v_dim_client_data"