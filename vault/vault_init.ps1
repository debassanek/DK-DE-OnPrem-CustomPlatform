# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script : vault_init.ps1
#  Description : Recree les secrets Vault en lisant les valeurs
#                depuis le fichier .env (coherence garantie)
#  Usage : .\vault\vault_init.ps1 -ComtradeKey "ta_cle"
#  Prerequis : fichier .env present a la racine du projet
# =================================================================

param(
    [string]$ComtradeKey = ""
)

$ErrorActionPreference = "Stop"

# ----------------------------------------------------------------
# Lecture du fichier .env
# ----------------------------------------------------------------
$envPath = ".env"
if (-not (Test-Path $envPath)) {
    Write-Host "ERREUR : fichier .env introuvable a la racine du projet." -ForegroundColor Red
    Write-Host "Copier .env.example vers .env et le remplir d'abord." -ForegroundColor Yellow
    exit 1
}

# Charge les variables du .env dans une table de hachage
$envVars = @{}
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $parts = $line -split "=", 2
        $envVars[$parts[0].Trim()] = $parts[1].Trim()
    }
}

# ----------------------------------------------------------------
# Variables Vault
# ----------------------------------------------------------------
$VaultToken = $envVars["VAULT_ROOT_TOKEN"]
$VaultAddr  = "http://127.0.0.1:8200"

# Cle API Comtrade : argument prioritaire, sinon .env, sinon placeholder
if (-not $ComtradeKey) {
    $ComtradeKey = $envVars["COMTRADE_API_KEY"]
}
if (-not $ComtradeKey) {
    $ComtradeKey = "CHANGEME"
    Write-Host "ATTENTION : aucune cle Comtrade fournie." -ForegroundColor Yellow
}

Write-Host "Initialisation des secrets Vault depuis .env..." -ForegroundColor Cyan

# ----------------------------------------------------------------
# Secret PostgreSQL (coherent avec DW_* du .env)
# ----------------------------------------------------------------
docker exec -e VAULT_TOKEN=$VaultToken -e VAULT_ADDR=$VaultAddr dkde_vault `
  vault kv put secret/postgres `
    username="$($envVars['DW_USER'])" `
    password="$($envVars['DW_PASSWORD'])" `
    host="postgres" `
    port="5432" `
    database="$($envVars['DW_DBNAME'])"

# ----------------------------------------------------------------
# Secret MinIO (coherent avec MINIO_* du .env)
# ----------------------------------------------------------------
docker exec -e VAULT_TOKEN=$VaultToken -e VAULT_ADDR=$VaultAddr dkde_vault `
  vault kv put secret/minio `
    access_key="$($envVars['MINIO_ROOT_USER'])" `
    secret_key="$($envVars['MINIO_ROOT_PASSWORD'])" `
    endpoint="http://minio:9000"

# ----------------------------------------------------------------
# Secret Comtrade
# ----------------------------------------------------------------
docker exec -e VAULT_TOKEN=$VaultToken -e VAULT_ADDR=$VaultAddr dkde_vault `
  vault kv put secret/comtrade `
    api_key="$ComtradeKey"

Write-Host ""
Write-Host "Secrets Vault crees avec succes :" -ForegroundColor Green
Write-Host "  - secret/postgres  (coherent avec DW_* du .env)"
Write-Host "  - secret/minio     (coherent avec MINIO_* du .env)"
Write-Host "  - secret/comtrade"