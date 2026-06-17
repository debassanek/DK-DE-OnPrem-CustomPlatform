# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Module : utils_vault.py
#  Description : Fonctions de récupération des secrets depuis Vault
# =================================================================

from airflow.providers.hashicorp.hooks.vault import VaultHook
import logging


def get_secret(secret_path):
    """
    Récupère un secret depuis HashiCorp Vault.

    Args:
        secret_path (str): chemin du secret (ex: 'postgres', 'minio')

    Returns:
        dict: les paires clé-valeur du secret
    """
    hook   = VaultHook(vault_conn_id='vault_default')
    secret = hook.get_secret(secret_path=secret_path)
    logging.info(f"Secret '{secret_path}' récupéré depuis Vault")
    return secret


def get_postgres_uri():
    """Construit l'URI PostgreSQL depuis les secrets Vault"""
    s = get_secret('postgres')
    return (
        f"postgresql+psycopg2://{s['username']}:{s['password']}"
        f"@{s['host']}:{s['port']}/{s['database']}"
    )


def get_minio_config():
    """Récupère la configuration MinIO depuis Vault"""
    s = get_secret('minio')
    return {
        'endpoint_url'         : s['endpoint'],
        'aws_access_key_id'    : s['access_key'],
        'aws_secret_access_key': s['secret_key']
    }


def get_comtrade_key():
    """Récupère la clé API Comtrade depuis Vault"""
    s = get_secret('comtrade')
    return s['api_key']

def get_dbt_env():
    """Retourne les variables d'environnement dbt depuis Vault.
    Ne bloque pas le parsing du DAG si Vault est indisponible."""
    try:
        s = get_secret('postgres')
        if not s:
            return {}
        return {
            'DW_USER'    : s['username'],
            'DW_PASSWORD': s['password'],
            'DW_DBNAME'  : s['database']
        }
    except Exception as e:
        logging.warning(f"Vault indisponible au parsing : {e}")
        return {}