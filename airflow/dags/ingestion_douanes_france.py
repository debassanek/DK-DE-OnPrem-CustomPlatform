# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script      : ingestion_douanes_france.py
#  Description : DAG d'ingestion des données de la douane française
#  Source      : data.gouv.fr
#  Secrets     : HashiCorp Vault (vault_default)
#  Fréquence   : 1er de chaque mois à 6h00
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
import logging

# Module utilitaire Vault (récupération des secrets au runtime)
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils_vault import get_postgres_uri, get_minio_config, get_dbt_env

# -----------------------------------------------------------------
# Constantes (NON sensibles uniquement)
# -----------------------------------------------------------------
URL_DOUANES  = "https://www.data.gouv.fr/api/1/datasets/r/39fca96a-740f-491c-9259-418f071ce2b0"
TMP_PATH     = "/tmp/"
CONN_ID      = "postgres_dw"
SOURCE_DATA  = "DATA_GOUV"
MINIO_BUCKET = 'bronze'

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
# Tâche 1 : telecharger_fichier
# -----------------------------------------------------------------
def telecharger_fichier():
    """Télécharge le fichier douanes depuis data.gouv.fr vers /tmp"""

    nom_fichier    = f"douanes_france_{datetime.now().strftime('%Y%m')}.csv"
    chemin_fichier = f"/tmp/{nom_fichier}"

    try:
        logging.info(f"Téléchargement depuis : {URL_DOUANES}")
        response = requests.get(URL_DOUANES, timeout=30)
        response.raise_for_status()

        with open(chemin_fichier, 'wb') as f:
            f.write(response.content)

        logging.info(f"Fichier sauvegardé dans : {chemin_fichier}")
        return chemin_fichier

    except Exception as e:
        logging.error(f"Erreur de téléchargement : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 2 : valider_fichier
# -----------------------------------------------------------------
def valider_fichier(**context):
    """Vérifie que le fichier téléchargé est exploitable"""

    chemin = context['ti'].xcom_pull(task_ids='telecharger_fichier')

    if not os.path.exists(chemin):
        logging.error(f"Fichier introuvable : {chemin}")
        raise FileNotFoundError(f"Fichier introuvable : {chemin}")

    if os.path.getsize(chemin) == 0:
        logging.error(f"Fichier vide : {chemin}")
        raise ValueError(f"Fichier vide : {chemin}")

    try:
        df = pd.read_csv(chemin, nrows=5, sep=';')
    except Exception as e:
        logging.error(f"Fichier illisible : {e}")
        raise

    if len(df) == 0:
        logging.error("Aucune ligne dans le fichier")
        raise ValueError("Fichier sans données")

    logging.info(f"Fichier valide - taille : {os.path.getsize(chemin)} octets")


# -----------------------------------------------------------------
# Tâche 3 : uploader_minio
# -----------------------------------------------------------------
def uploader_minio(**context):
    """Upload le fichier CSV vers MinIO bucket bronze"""

    chemin      = context['ti'].xcom_pull(task_ids='telecharger_fichier')
    s3_client   = _minio_client()
    nom_fichier = os.path.basename(chemin)

    try:
        s3_client.upload_file(chemin, MINIO_BUCKET, nom_fichier)
        logging.info(f"Fichier uploadé dans MinIO : {MINIO_BUCKET}/{nom_fichier}")
        return f"{MINIO_BUCKET}/{nom_fichier}"

    except Exception as e:
        logging.error(f"Erreur upload MinIO : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 4 : charger_bronze
# -----------------------------------------------------------------
def charger_bronze(**context):
    """Lit le fichier depuis MinIO et charge dans PostgreSQL Bronze"""

    chemin_minio = context['ti'].xcom_pull(task_ids='uploader_minio')

    s3_client   = _minio_client()
    bucket      = chemin_minio.split('/')[0]
    nom_fichier = chemin_minio.split('/')[1]
    chemin_tmp  = f"/tmp/{nom_fichier}"

    s3_client.download_file(bucket, nom_fichier, chemin_tmp)
    logging.info(f"Fichier téléchargé depuis MinIO : {chemin_minio}")

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    total_lignes = 0

    try:
        for chunk in pd.read_csv(chemin_tmp, chunksize=50000, sep=';'):

            chunk['source_systeme'] = SOURCE_DATA
            chunk['source_fichier'] = os.path.basename(chemin_tmp)
            chunk['source_url']     = URL_DOUANES
            chunk['date_ingestion'] = datetime.now()

            chunk = chunk.rename(columns={
                'flux'      : 'sens_flux',
                'mois'      : 'mois_declaration',
                'annee'     : 'annee_declaration',
                'code-nc8'  : 'code_hs_marchandise',
                'code-pays' : 'code_pays_destination',
                'valeur'    : 'valeur_brute',
                'masse'     : 'poids_brut'
            })

            colonnes = [
                'source_systeme', 'source_fichier', 'source_url',
                'date_ingestion', 'sens_flux', 'mois_declaration',
                'annee_declaration', 'code_hs_marchandise',
                'code_pays_destination', 'valeur_brute', 'poids_brut'
            ]

            valeurs = [tuple(row) for row in chunk[colonnes].values]

            execute_values(cursor, """
                INSERT INTO bronze.stg_declarations_douane
                    (source_systeme, source_fichier, source_url,
                     date_ingestion, sens_flux, mois_declaration,
                     annee_declaration, code_hs_marchandise,
                     code_pays_destination, valeur_brute, poids_brut)
                VALUES %s
            """, valeurs, page_size=5000)

            conn.commit()
            total_lignes += len(chunk)
            logging.info(f"{total_lignes} lignes insérées...")

        logging.info(f"Chargement terminé : {total_lignes} lignes")
        return total_lignes

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur chargement Bronze : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 8 : exporter_silver_parquet
# -----------------------------------------------------------------
def exporter_silver_parquet(**context):
    """Exporte silver.declarations_douane vers MinIO en Parquet par chunks"""

    from sqlalchemy import create_engine
    import pyarrow as pa
    import pyarrow.parquet as pq

    engine = create_engine(get_postgres_uri())

    nom_fichier  = f"declarations_douane_{datetime.now().strftime('%Y%m')}.parquet"
    chemin_tmp   = f"/tmp/{nom_fichier}"
    writer       = None
    total_lignes = 0

    try:
        query = """
            SELECT
                id_source, sens_flux, date_declaration,
                code_pays_destination, code_hs_marchandise,
                valeur_euro, poids_kg_net, ligne_valide
            FROM silver.declarations_douane
        """

        for chunk in pd.read_sql(query, engine, chunksize=50000):
            table = pa.Table.from_pandas(chunk)
            if writer is None:
                writer = pq.ParquetWriter(chemin_tmp, table.schema)
            writer.write_table(table)
            total_lignes += len(chunk)
            logging.info(f"{total_lignes} lignes exportées...")

        if writer:
            writer.close()

        logging.info(f"Parquet créé : {chemin_tmp}")

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
        logging.error(f"Erreur export Silver : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 9 : exporter_gold_parquet
# -----------------------------------------------------------------
def exporter_gold_parquet(**context):
    """Exporte les tables Gold vers MinIO en Parquet"""

    from sqlalchemy import create_engine
    import pyarrow as pa
    import pyarrow.parquet as pq

    engine = create_engine(get_postgres_uri())
    s3_client = _minio_client()

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    tables_simples = ['dim_date', 'dim_pays', 'dim_marchandise']
    tables_lourdes = ['fact_flux_commercial']

    try:
        for table in tables_simples:
            logging.info(f"Export Gold : {table}...")
            df          = pd.read_sql(f"SELECT * FROM gold.{table}", engine)
            nom_fichier = f"{table}_{datetime.now().strftime('%Y%m')}.parquet"
            chemin_tmp  = f"/tmp/{nom_fichier}"
            df.to_parquet(chemin_tmp, index=False)
            s3_client.upload_file(chemin_tmp, 'gold', nom_fichier)
            taille = os.path.getsize(chemin_tmp)
            cursor.execute("""
                INSERT INTO audit.audit_fichiers_minio
                    (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (context['dag'].dag_id, 'gold', nom_fichier, 'PARQUET', taille, len(df)))
            conn.commit()
            logging.info(f"Exporté : gold/{nom_fichier} : {taille} octets")

        for table in tables_lourdes:
            logging.info(f"Export Gold chunks : {table}...")
            nom_fichier  = f"{table}_{datetime.now().strftime('%Y%m')}.parquet"
            chemin_tmp   = f"/tmp/{nom_fichier}"
            writer       = None
            total_lignes = 0

            for chunk in pd.read_sql(
                f"SELECT * FROM gold.{table}", engine, chunksize=50000
            ):
                t = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(chemin_tmp, t.schema)
                writer.write_table(t)
                total_lignes += len(chunk)
                del chunk, t

            if writer:
                writer.close()

            s3_client.upload_file(chemin_tmp, 'gold', nom_fichier)
            taille = os.path.getsize(chemin_tmp)
            cursor.execute("""
                INSERT INTO audit.audit_fichiers_minio
                    (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (context['dag'].dag_id, 'gold', nom_fichier, 'PARQUET', taille, total_lignes))
            conn.commit()
            logging.info(f"Exporté : gold/{nom_fichier} : {taille} octets")

        return "gold export OK"

    except Exception as e:
        logging.error(f"Erreur export Gold : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 10 : truncate_bronze_silver
# -----------------------------------------------------------------
def truncate_bronze_silver(**context):
    """Vide les tables temporaires Bronze et Silver"""

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    tables = [
        'bronze.stg_declarations_douane',
        'silver.declarations_douane'
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
        logging.info("Bronze et Silver vidés avec succès")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur truncate : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 11 : logger_execution
# -----------------------------------------------------------------
def logger_execution(**context):
    """Enregistre le résultat du pipeline dans audit.audit_pipelines"""

    nb_lignes      = context['ti'].xcom_pull(task_ids='charger_bronze')
    chemin_fichier = context['ti'].xcom_pull(task_ids='telecharger_fichier')
    nom_dag        = context['dag'].dag_id
    nom_tache      = context['task'].task_id
    date_debut     = context['data_interval_start']
    date_fin       = datetime.now()

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
            nb_lignes, os.path.basename(chemin_fichier), 'airflow'
        ))

        conn.commit()
        logging.info("Log d'exécution enregistré avec succès")

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
    dag_id            = 'ingestion_douanes_france',
    schedule_interval = '0 6 1 * *',
    start_date        = datetime(2026, 6, 1),
    catchup           = False,
    tags              = ['ingestion', 'douane', 'bronze', 'vault']
) as dag:

    t1 = PythonOperator(
        task_id         = 'telecharger_fichier',
        python_callable = telecharger_fichier
    )

    t2 = PythonOperator(
        task_id         = 'valider_fichier',
        python_callable = valider_fichier,
        provide_context = True
    )

    t3 = PythonOperator(
        task_id         = 'uploader_minio',
        python_callable = uploader_minio,
        provide_context = True
    )

    t4 = PythonOperator(
        task_id         = 'charger_bronze',
        python_callable = charger_bronze,
        provide_context = True
    )

    t5 = DockerOperator(
        task_id        = 'run_dbt_silver',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_silver',
        command        = 'run --select silver --project-dir /usr/app/dbt --profiles-dir /root/.dbt',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t6 = DockerOperator(
        task_id        = 'run_dbt_gold',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_gold',
        command        = 'run --select gold --project-dir /usr/app/dbt --profiles-dir /root/.dbt',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t7 = DockerOperator(
        task_id        = 'dbt_test',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_test',
        command        = 'test --project-dir /usr/app/dbt --profiles-dir /root/.dbt',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t8 = PythonOperator(
        task_id         = 'exporter_silver_parquet',
        python_callable = exporter_silver_parquet,
        provide_context = True
    )

    t9 = PythonOperator(
        task_id         = 'exporter_gold_parquet',
        python_callable = exporter_gold_parquet,
        provide_context = True
    )

    t10 = PythonOperator(
        task_id         = 'truncate_bronze_silver',
        python_callable = truncate_bronze_silver,
        provide_context = True
    )

    t11 = PythonOperator(
        task_id         = 'logger_execution',
        python_callable = logger_execution,
        provide_context = True
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11