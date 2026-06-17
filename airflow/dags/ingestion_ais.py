# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script      : ingestion_ais.py
#  Description : DAG d'ingestion AIS incrémental (logistique portuaire)
#  Source      : Générateur AIS simulé (mode hebdomadaire)
#  Secrets     : HashiCorp Vault (vault_default)
#  Stratégie   : Bronze/Silver temporaires, Gold incrémental (append)
#  Fréquence   : hebdomadaire (tous les lundis à 7h00)
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
import pandas as pd
import os
import json
import logging

# Module utilitaire Vault
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils_vault import get_postgres_uri, get_minio_config, get_dbt_env

# Générateur AIS hebdomadaire
sys.path.insert(0, '/opt/airflow/python/simulateurs')
from generate_ais_data import generer_dataset_semaine

# -----------------------------------------------------------------
# Constantes (NON sensibles uniquement)
# -----------------------------------------------------------------
CONN_ID     = "postgres_dw"
SOURCE_DATA = "SIMULATED_AIS_WEEKLY"

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
# Tâche 1 : generer_ais_semaine
# -----------------------------------------------------------------
def generer_ais_semaine(**context):
    """Génère les mouvements AIS de la semaine (basée sur data_interval_start)"""

    date_debut = context['data_interval_start']
    date_debut = datetime(date_debut.year, date_debut.month, date_debut.day)

    logging.info(f"Génération AIS pour la semaine du {date_debut.strftime('%Y-%m-%d')}")

    chemin, data = generer_dataset_semaine(date_debut, nb_navires=850)

    nb = data['metadata']['nb_mouvements']
    logging.info(f"{nb} mouvements générés : {chemin}")

    return {
        'chemin'       : chemin,
        'semaine'      : date_debut.strftime('%Y-%m-%d'),
        'nb_mouvements': nb
    }


# -----------------------------------------------------------------
# Tâche 2 : uploader_minio
# -----------------------------------------------------------------
def uploader_minio(**context):
    """Upload le JSON AIS vers MinIO bronze (partitionné par semaine)"""

    info    = context['ti'].xcom_pull(task_ids='generer_ais_semaine')
    chemin  = info['chemin']
    semaine = info['semaine']

    s3_client = _minio_client()
    nom_objet = f"ais/semaine={semaine}/ais_mouvements.json"

    try:
        s3_client.upload_file(chemin, 'bronze', nom_objet)
        logging.info(f"Fichier uploadé : bronze/{nom_objet}")

        hook   = PostgresHook(postgres_conn_id=CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()
        taille = os.path.getsize(chemin)
        cursor.execute("""
            INSERT INTO audit.audit_fichiers_minio
                (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            context['dag'].dag_id, 'bronze', nom_objet, 'JSON',
            taille, info['nb_mouvements']
        ))
        conn.commit()
        cursor.close()
        conn.close()

        return f"bronze/{nom_objet}"

    except Exception as e:
        logging.error(f"Erreur upload MinIO : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 3 : charger_bronze
# -----------------------------------------------------------------
def charger_bronze(**context):
    """TRUNCATE puis charge les mouvements de la semaine dans Bronze"""

    info   = context['ti'].xcom_pull(task_ids='generer_ais_semaine')
    chemin = info['chemin']

    with open(chemin, 'r', encoding='utf-8') as f:
        data = json.load(f)

    mouvements = data['mouvements']

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE bronze.stg_mouvements_navires;")
        logging.info("Bronze vidé avant chargement de la semaine")

        valeurs = []
        for m in mouvements:
            valeurs.append((
                SOURCE_DATA,
                'ais_mouvements.json',
                None,
                datetime.now(),
                str(m['mmsi']),
                str(m['imo']),
                m['nom_navire'],
                str(m['latitude']),
                str(m['longitude']),
                str(m['vitesse_noeuds']),
                m['port_destination'],
                m['timestamp_position'],
                m['statut_navigation']
            ))

        execute_values(cursor, """
            INSERT INTO bronze.stg_mouvements_navires
                (source_systeme, source_fichier, source_url, date_ingestion,
                 numero_mmsi, numero_imo, nom_navire,
                 latitude_brute, longitude_brute, vitesse_noeud,
                 port_destination, temps_position, statut_navigation)
            VALUES %s
        """, valeurs, page_size=5000)

        conn.commit()
        total = len(valeurs)
        logging.info(f"Chargé en Bronze : {total} mouvements")
        return total

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur chargement Bronze : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 4 : valider_insertion
# -----------------------------------------------------------------
def valider_insertion(**context):
    """Vérifie que les mouvements de la semaine sont bien dans Bronze"""

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*), MIN(temps_position), MAX(temps_position)
            FROM bronze.stg_mouvements_navires
        """)
        count, min_t, max_t = cursor.fetchone()

        if count == 0:
            logging.error("Aucune donnée AIS trouvée en Bronze")
            raise ValueError("Insertion AIS échouée : aucune ligne")

        logging.info(f"Validation OK : {count} mouvements | {min_t} -> {max_t}")
        return count

    except Exception as e:
        logging.error(f"Erreur validation : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------------------------
# Tâche 8 : exporter_silver_parquet
# -----------------------------------------------------------------
def exporter_silver_parquet(**context):
    """Exporte silver.mouvements_navires vers MinIO en Parquet"""

    from sqlalchemy import create_engine

    engine = create_engine(get_postgres_uri())

    info       = context['ti'].xcom_pull(task_ids='generer_ais_semaine')
    semaine    = info['semaine']
    nom_objet  = f"ais/semaine={semaine}/mouvements_navires.parquet"
    chemin_tmp = "/tmp/mouvements_navires_semaine.parquet"

    try:
        df = pd.read_sql("SELECT * FROM silver.mouvements_navires", engine)
        df.to_parquet(chemin_tmp, index=False)

        s3_client = _minio_client()
        s3_client.upload_file(chemin_tmp, 'silver', nom_objet)

        hook   = PostgresHook(postgres_conn_id=CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()
        taille = os.path.getsize(chemin_tmp)
        cursor.execute("""
            INSERT INTO audit.audit_fichiers_minio
                (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (context['dag'].dag_id, 'silver', nom_objet, 'PARQUET', taille, len(df)))
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Exporté : silver/{nom_objet} : {len(df)} lignes")
        return f"silver/{nom_objet}"

    except Exception as e:
        logging.error(f"Erreur export Silver : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 9 : exporter_gold_parquet
# -----------------------------------------------------------------
def exporter_gold_parquet(**context):
    """Exporte fact_mouvement_conteneur (complet) vers MinIO en Parquet"""

    from sqlalchemy import create_engine
    import pyarrow as pa
    import pyarrow.parquet as pq

    engine = create_engine(get_postgres_uri())

    nom_fichier  = f"fact_mouvement_conteneur_{datetime.now().strftime('%Y%m%d')}.parquet"
    chemin_tmp   = f"/tmp/{nom_fichier}"
    writer       = None
    total_lignes = 0

    try:
        for chunk in pd.read_sql(
            "SELECT * FROM gold.fact_mouvement_conteneur", engine, chunksize=50000
        ):
            t = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(chemin_tmp, t.schema)
            writer.write_table(t)
            total_lignes += len(chunk)
            del chunk, t

        if writer:
            writer.close()

        s3_client = _minio_client()
        s3_client.upload_file(chemin_tmp, 'gold', nom_fichier)

        hook   = PostgresHook(postgres_conn_id=CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()
        taille = os.path.getsize(chemin_tmp)
        cursor.execute("""
            INSERT INTO audit.audit_fichiers_minio
                (nom_dag, bucket, nom_fichier, format, taille_octets, nb_lignes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (context['dag'].dag_id, 'gold', nom_fichier, 'PARQUET', taille, total_lignes))
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Exporté : gold/{nom_fichier} : {total_lignes} lignes")
        return f"gold/{nom_fichier}"

    except Exception as e:
        logging.error(f"Erreur export Gold : {e}")
        raise


# -----------------------------------------------------------------
# Tâche 10 : truncate_bronze_silver
# -----------------------------------------------------------------
def truncate_bronze_silver(**context):
    """Vide Bronze et Silver (Gold reste permanent et incrémental)"""

    hook   = PostgresHook(postgres_conn_id=CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    tables = [
        'bronze.stg_mouvements_navires',
        'silver.mouvements_navires'
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
        logging.info("Bronze et Silver vidés (Gold conservé)")

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

    nb_lignes  = context['ti'].xcom_pull(task_ids='charger_bronze')
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
            nb_lignes, 'ais_mouvements.json', 'airflow'
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
    dag_id            = 'ingestion_ais',
    schedule_interval = '0 7 * * 1',
    start_date        = datetime(2025, 1, 6),
    catchup           = False,
    tags              = ['ingestion', 'ais', 'logistique', 'incremental', 'vault']
) as dag:

    t1 = PythonOperator(
        task_id         = 'generer_ais_semaine',
        python_callable = generer_ais_semaine,
        provide_context = True
    )

    t2 = PythonOperator(
        task_id         = 'uploader_minio',
        python_callable = uploader_minio,
        provide_context = True
    )

    t3 = PythonOperator(
        task_id         = 'charger_bronze',
        python_callable = charger_bronze,
        provide_context = True
    )

    t4 = PythonOperator(
        task_id         = 'valider_insertion',
        python_callable = valider_insertion,
        provide_context = True
    )

    t5 = DockerOperator(
        task_id        = 'run_dbt_silver',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_silver_ais',
        command        = 'run --select mouvements_navires --project-dir /usr/app/dbt --profiles-dir /root/.dbt --no-partial-parse',
        environment    = get_dbt_env(),
        mounts         = DBT_MOUNTS,
        network_mode   = 'dkde-network',
        auto_remove    = True,
        docker_url     = 'unix://var/run/docker.sock',
        dag            = dag
    )

    t6 = DockerOperator(
        task_id        = 'run_dbt_gold_incremental',
        image          = 'ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        container_name = 'airflow_dbt_gold_ais',
        command        = 'run --select dim_navire dim_port fact_mouvement_conteneur --project-dir /usr/app/dbt --profiles-dir /root/.dbt --no-partial-parse',
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
        container_name = 'airflow_dbt_test_ais',
        command        = 'test --select mouvements_navires dim_navire dim_port fact_mouvement_conteneur --project-dir /usr/app/dbt --profiles-dir /root/.dbt --no-partial-parse',
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