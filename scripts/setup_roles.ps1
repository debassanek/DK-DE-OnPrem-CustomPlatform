# =================================================================
# DK-DE-OnPrem-CustomPlatform
# Script : setup_roles.ps1
# Description : Applique la couche de gouvernance (roles, permissions,
#               RLS, vues securisees) sur la base Data Warehouse.
#               Lit les mots de passe depuis .env.
# Usage : .\scripts\setup_roles.ps1
# =================================================================

$ErrorActionPreference = "Stop"

# ----------------------------------------------------------------
# Lecture du .env
# ----------------------------------------------------------------
$envPath = ".env"
if (-not (Test-Path $envPath)) {
    Write-Host "ERREUR : fichier .env introuvable." -ForegroundColor Red
    exit 1
}

$envVars = @{}
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $parts = $line -split "=", 2
        $envVars[$parts[0].Trim()] = $parts[1].Trim()
    }
}

$DB_CONTAINER = "dkde_postgres"
$DB_SUPERUSER = $envVars["POSTGRES_USER"]
$DB_NAME      = $envVars["DW_DBNAME"]
$SQL_DIR      = "/docker-entrypoint-initdb.d"

Write-Host "=== Application de la couche de gouvernance ===" -ForegroundColor Cyan
Write-Host "Base : $DB_NAME"

# ----------------------------------------------------------------
# 03 : Roles et comptes techniques
# ----------------------------------------------------------------
Write-Host ">> 03 : creation des roles et comptes..." -ForegroundColor Yellow
docker exec -i $DB_CONTAINER psql `
    -v ON_ERROR_STOP=1 `
    -v AIRFLOW_PASSWORD="$($envVars['AIRFLOW_PASSWORD'])" `
    -v PBI_SERVICE_PASSWORD="$($envVars['PBI_SERVICE_PASSWORD'])" `
    -v MONITORING_PASSWORD="$($envVars['MONITORING_PASSWORD'])" `
    -v DATA_ACCESS_PASSWORD="$($envVars['DATA_ACCESS_PASSWORD'])" `
    -U $DB_SUPERUSER -d $DB_NAME `
    -f "$SQL_DIR/03_roles_securite.sql"

# ----------------------------------------------------------------
# 04 : Permissions
# ----------------------------------------------------------------
Write-Host ">> 04 : attribution des permissions..." -ForegroundColor Yellow
docker exec -i $DB_CONTAINER psql `
    -v ON_ERROR_STOP=1 `
    -U $DB_SUPERUSER -d $DB_NAME `
    -f "$SQL_DIR/04_permissions.sql"

# ----------------------------------------------------------------
# 05 : Row Level Security
# ----------------------------------------------------------------
Write-Host ">> 05 : activation de la RLS..." -ForegroundColor Yellow
docker exec -i $DB_CONTAINER psql `
    -v ON_ERROR_STOP=1 `
    -U $DB_SUPERUSER -d $DB_NAME `
    -f "$SQL_DIR/05_rls.sql"

# ----------------------------------------------------------------
# 06 : Vues securisees
# ----------------------------------------------------------------
Write-Host ">> 06 : creation des vues securisees..." -ForegroundColor Yellow
docker exec -i $DB_CONTAINER psql `
    -v ON_ERROR_STOP=1 `
    -U $DB_SUPERUSER -d $DB_NAME `
    -f "$SQL_DIR/06_vues_securisees.sql"

Write-Host ""
Write-Host "=== Gouvernance appliquee avec succes ===" -ForegroundColor Green
Write-Host "  - Roles : role_pipeline, role_powerbi, role_data"
Write-Host "  - Comptes : user_airflow, user_pbi_service, user_monitoring, user_data"
Write-Host "  - RLS active sur les 4 tables de faits Gold"
Write-Host "  - Vue securisee : gold.v_dim_client_data"