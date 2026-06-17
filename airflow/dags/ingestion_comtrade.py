# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script      : ingestion_comtrade.py
#  Description : DAG d'ingestion des flux commerciaux UN Comtrade
#  Source      : comtradeapi.un.org
#  Secrets     : HashiCorp Vault (vault_default)
#  Fréquence   : 1er de chaque mois à 8h00
# =================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from docker.types import Mount
from psycopg2.extras import execute_values
from datetime import datetime
from botocore.client import Config
import boto3
import requests
import pandas as pd
import os
import json
import logging

# Module utilitaire Vault (récupération des secrets au runtime)
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils_vault import get_postgres_uri, get_minio_config, get_comtrade_key, get_dbt_env

# -----------------------------------------------------------------
# Constantes (NON sensibles uniquement)
# -----------------------------------------------------------------
URL_COMTRADE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
CONN_ID      = "postgres_dw"
SOURCE_DATA  = "COMTRADE"
BRONZE_PATH  = "/tmp/"

DBT_SOURCE  = 'C:\\Users\\kpan_\\Desktop\\FORMATION\\DATA ENGINEERING\\On premise\\DK-DE-OnPrem-CustomPlatform\\dbt'
DBT_PROFILE = 'C:\\Users\\kpan_\\Desktop\\FORMATION\\DATA ENGINEERING\\On premise\\DK-DE-OnPrem-CustomPlatform\\dbt\\profiles.yml'

DBT_MOUNTS = [
    Mount(source=DBT_SOURCE,  target='/usr/app/dbt',            type='bind'),
    Mount(source=DBT_PROFILE, target='/root/.dbt/profiles.yml', type='bind')
]


def _minio_client():
    """Construit un client boto3 MinIO à partir des secrets Vault"""
    cfg = get_minio_config()
    return boto3.client(
        's3',
        config = Config(signature_version='s3v4'),
        **cfg
    )


# -----------------------------------------------------------------
# Tâche 1 : telecharger_comtrade
# -----------------------------------------------------------------
def telecharger_comtrade(**context):
    """Télécharge les flux Comtrade et charge dans Bronze"""

    API_KEY   = get_comtrade_key()
    s3_client = _minio_client()

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT DISTINCT d.annee
        FROM gold.fact_flux_commercial f
        JOIN gold.dim_date d ON d.id_date = f.id_date
        WHERE d.annee IS NOT NULL
        ORDER BY d.annee
                """)
        annees = [str(row[0]) for row in cursor.fetchall()]
        logging.info(f"Années détectées dans Gold : {annees}")

        total_lignes = 0

        for annee in annees:
            for flux in ['X', 'M']:
                logging.info(f"Téléchargement Comtrade {annee}/{flux}...")

                params = {
                    'reporterCode'     : '251',
                    'period'           : annee,
                    'partnerCode'      : '0',
                    'cmdCode'          : 'AG2',
                    'flowCode'         : flux,
                    'maxRecords'       : '250000',
                    'subscription-key' : API_KEY
                }

                response = requests.get(URL_COMTRADE, params=params, timeout=60)
                response.raise_for_status()

                data            = response.json()
                enregistrements = data.get('data', [])
                nb              = data.get('count', 0)
                logging.info(f"{nb} enregistrements pour {annee}/{flux}")

                nom_fichier    = f"comtrade_{annee}_{flux}.json"
                chemin_fichier = f"{BRONZE_PATH}{nom_fichier}"
                with open(chemin_fichier, 'w', encoding='utf-8') as f:
                    json.dump(data, f)

                s3_client.upload_file(chemin_fichier, 'bronze', nom_fichier)
                logging.info(f"Uploadé MinIO : bronze/{nom_fichier}")

                if enregistrements:
                    valeurs = []
                    for record in enregistrements:
                        valeurs.append((
                            SOURCE_DATA,
                            nom_fichier,
                            str(record.get('period')),
                            str(record.get('reporterCode')),
                            str(record.get('partnerCode')),
                            record.get('flowCode'),
                            str(record.get('cmdCode')),
                            record.get('primaryValue'),
                            record.get('netWgt'),
                            json.dumps(record)
                        ))

                    execute_values(cursor, """
                        INSERT INTO bronze.stg_flux_commerciaux
                            (source_systeme, source_fichier, periode,
                             code_pays_exportateur, code_pays_importateur,
                             sens_flux, code_hs_marchandise,
                             valeur_usd_brute, poids_kg_brut, raw_json)
                        VALUES %s
                    """, valeurs, page_size=5000)

                    conn.commit()
                    total_lignes += len(valeurs)
                    logging.info(f"Total inséré : {total_lignes} lignes")

        logging.info(f"Chargement terminé : {total_lignes} lignes")
        return total_lignes

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur Comtrade : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 2 : valider_insertion
# -----------------------------------------------------------------
def valider_insertion(**context):
    """Vérifie que les données Comtrade sont bien dans Bronze"""

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*), MIN(periode), MAX(periode)
            FROM bronze.stg_flux_commerciaux
            WHERE source_systeme = 'COMTRADE'
        """)
        count, min_periode, max_periode = cursor.fetchone()

        if count == 0:
            logging.error("Aucune donnée Comtrade trouvée en Bronze")
            raise ValueError("Insertion Comtrade échouée : aucune ligne")

        logging.info(
            f"Validation OK : {count} lignes "
            f"| Périodes : {min_periode} -> {max_periode}"
        )
        return count

    except Exception as e:
        logging.error(f"Erreur validation : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 5 : exporter_silver_comtrade_parquet
# -----------------------------------------------------------------
def exporter_silver_comtrade_parquet(**context):
    """Exporte silver.flux_commerciaux vers MinIO en Parquet"""

    from sqlalchemy import create_engine
    import pyarrow as pa
    import pyarrow.parquet as pq

    engine = create_engine(get_postgres_uri())

    nom_fichier  = f"flux_commerciaux_{datetime.now().strftime('%Y%m')}.parquet"
    chemin_tmp   = f"/tmp/{nom_fichier}"
    writer       = None
    total_lignes = 0

    try:
        query = "SELECT * FROM silver.flux_commerciaux"

        for chunk in pd.read_sql(query, engine, chunksize=50000):
            table = pa.Table.from_pandas(chunk)
            if writer is None:
                writer = pq.ParquetWriter(chemin_tmp, table.schema)
            writer.write_table(table)
            total_lignes += len(chunk)
            logging.info(f"{total_lignes} lignes exportées...")

        if writer:
            writer.close()

        s3_client = _minio_client()
        s3_client.upload_file(chemin_tmp, 'silver', nom_fichier)

        hook   = PostgresHook(postgres_conn_id=CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()
        taille = os.path.getsize(chemin_tmp)

        cursor.execute("""
            INSERT INTO audit.audit_fichiers_minio
                (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            context['dag'].dag_id,
            'silver', nom_fichier, 'PARQUET',
            taille, total_lignes
        ))
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Exporté : silver/{nom_fichier} : {taille} octets")
        return f"silver/{nom_fichier}"

    except Exception as e:
        logging.error(f"Erreur export Silver Comtrade : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 6 : truncate_bronze_comtrade
# -----------------------------------------------------------------
def truncate_bronze_comtrade(**context):
    """Vide les tables temporaires Bronze et Silver Comtrade"""

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    tables = [
        'bronze.stg_flux_commerciaux',
        'silver.flux_commerciaux'
    ]

    try:
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table};")
            logging.info(f"Table vidée : {table}")

            cursor.execute("""
                INSERT INTO audit.audit_pipelines
                    (nom_dag, nom_tache, statut_execution,
                     date_debut, date_fin, message_execution)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                context['dag'].dag_id,
                f'truncate_{table}',
                'SUCCES',
                datetime.now(),
                datetime.now(),
                f'Table {table} vidée avec succès'
            ))

        conn.commit()
        logging.info("Bronze et Silver Comtrade vidés avec succès")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur truncate : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 7 : logger_execution
# -----------------------------------------------------------------
def logger_execution(**context):
    """Enregistre le résultat du pipeline dans audit.audit_pipelines"""

    nb_lignes  = context['ti'].xcom_pull(task_ids='telecharger_comtrade')
    nom_dag    = context['dag'].dag_id
    nom_tache  = context['task'].task_id
    date_debut = context['data_interval_start']
    date_fin   = datetime.now()

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO audit.audit_pipelines
                (nom_dag, nom_tache, statut_execution,
                 date_debut, date_fin,
                 nb_lignes_inserees, source_fichier,
                 utilisateur_systeme)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            nom_dag, nom_tache, 'SUCCES',
            date_debut, date_fin,
            nb_lignes, 'comtrade_api', 'airflow'
        ))

        conn.commit()
        logging.info("Log enregistré avec succès")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur enregistrement log : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Définition du DAG
# -----------------------------------------------------------------
with DAG(
    dag_id            = 'ingestion_comtrade',
    schedule_interval = '0 8 1 * *',
    start_date        = datetime(2026, 6, 1),
    catchup           = False,
    tags              = ['ingestion', 'comtrade', 'bronze', 'vault']
) as dag:

    t1 = PythonOperator(
        task_id         = 'telecharger_comtrade',
        python_callable = telecharger_comtrade,
        provide_context = True
    )

    t2 = PythonOperator(
        task_id         = 'valider_insertion',
        python_callable = valider_insertion,
        provide_context = True
    )

    t3 = DockerOperator(
        task_id        = 'run_dbt_silver',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_silver_comtrade',
        command        = 'run --select flux_commerciaux --project-dir /usr/app/dbt --profiles-dir /root/.dbt --no-partial-parse',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t4 = DockerOperator(
        task_id        = 'dbt_test',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_test_comtrade',
        command        = 'test --select flux_commerciaux --project-dir /usr/app/dbt --profiles-dir /root/.dbt --no-partial-parse',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t5 = PythonOperator(
        task_id         = 'exporter_silver_parquet',
        python_callable = exporter_silver_comtrade_parquet,
        provide_context = True
    )

    t6 = PythonOperator(
        task_id         = 'truncate_bronze_silver',
        python_callable = truncate_bronze_comtrade,
        provide_context = True
    )

    t7 = PythonOperator(
        task_id         = 'logger_execution',
        python_callable = logger_execution,
        provide_context = True
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7