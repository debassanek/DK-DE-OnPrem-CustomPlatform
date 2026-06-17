#!/bin/bash
# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script : vault_init.sh
#  Description : Recrée les secrets Vault en lisant les valeurs
#                depuis le fichier .env (cohérence garantie)
#  Usage : bash vault/vault_init.sh
#  Prérequis : fichier .env présent à la racine du projet
# =================================================================
set -e

# ----------------------------------------------------------------
# Chargement des variables depuis .env
# ----------------------------------------------------------------
if [ ! -f .env ]; then
    echo "ERREUR : fichier .env introuvable à la racine du projet."
    echo "Copier .env.example vers .env et le remplir d'abord."
    exit 1
fi

# Exporte les variables du .env (ignore commentaires et lignes vides)
set -a
. ./.env
set +a

export VAULT_ADDR="http://127.0.0.1:8200"
export VAULT_TOKEN="${VAULT_ROOT_TOKEN}"

echo "Initialisation des secrets Vault depuis .env..."

# ----------------------------------------------------------------
# Secret PostgreSQL (cohérent avec DW_* du .env)
# ----------------------------------------------------------------
docker exec -e VAULT_TOKEN="${VAULT_TOKEN}" -e VAULT_ADDR="${VAULT_ADDR}" dkde_vault \
  vault kv put secret/postgres \
    username="${DW_USER}" \
    password="${DW_PASSWORD}" \
    host="postgres" \
    port="5432" \
    database="${DW_DBNAME}"

# ----------------------------------------------------------------
# Secret MinIO (cohérent avec MINIO_* du .env)
# ----------------------------------------------------------------
docker exec -e VAULT_TOKEN="${VAULT_TOKEN}" -e VAULT_ADDR="${VAULT_ADDR}" dkde_vault \
  vault kv put secret/minio \
    access_key="${MINIO_ROOT_USER}" \
    secret_key="${MINIO_ROOT_PASSWORD}" \
    endpoint="http://minio:9000"

# ----------------------------------------------------------------
# Secret Comtrade
# ----------------------------------------------------------------
COMTRADE_KEY="${1:-${COMTRADE_API_KEY:-CHANGEME}}"
docker exec -e VAULT_TOKEN="${VAULT_TOKEN}" -e VAULT_ADDR="${VAULT_ADDR}" dkde_vault \
  vault kv put secret/comtrade \
    api_key="${COMTRADE_KEY}"

echo ""
echo "Secrets Vault créés avec succès :"
echo "  - secret/postgres  (cohérent avec DW_* du .env)"
echo "  - secret/minio     (cohérent avec MINIO_* du .env)"
echo "  - secret/comtrade"