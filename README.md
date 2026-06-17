# DK-DE-OnPrem-CustomPlatform

Plateforme analytique **on-premise** sur données logistiques portuaires, douanières et de commerce international. Projet Data Engineering démontrant une architecture Lakehouse complète sans dépendance cloud, avec une couche de gouvernance et de sécurité des données.

---

## Description

Le projet construit un pipeline de données de bout en bout autour de deux domaines métier distincts : le **commerce extérieur** (statistiques douanières et flux internationaux) et la **logistique portuaire** (mouvements de navires). L'ensemble tourne en conteneurs dans un environnement local, orchestré par Airflow, transformé par dbt, stocké dans un Data Lake MinIO et un Data Warehouse PostgreSQL, avec gestion centralisée des secrets via HashiCorp Vault et une sécurité au niveau des lignes (Row Level Security).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       SOURCES EXTERNES                            │
│   data.gouv.fr (CSV)    UN Comtrade API (JSON)    AIS (simulé)    │
└──────────────────┬────────────────┬───────────────┬──────────────┘
                   │                │               │
                   ▼                ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION : Apache Airflow                  │
│   ingestion_douanes_france   ingestion_comtrade   ingestion_ais  │
└──────────────────┬────────────────┬───────────────┬──────────────┘
                   │                │               │
                   ▼                ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATA LAKE : MinIO (S3)                       │
│   bronze/  (CSV + JSON bruts)                                     │
│   silver/  (Parquet nettoyés)                                     │
│   gold/    (Parquet analytiques)                                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│            STAGING TEMPORAIRE : PostgreSQL Bronze / Silver        │
│            (vidé après chaque run via TRUNCATE)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt (transformations + tests)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│           DATA WAREHOUSE : PostgreSQL Gold (permanent)           │
│   Dimensions : dim_date, dim_pays, dim_marchandise,              │
│                dim_client, dim_navire, dim_port                   │
│   Faits      : fact_flux_commercial, fact_importation,           │
│                fact_exportation, fact_recettes_douane,           │
│                fact_mouvement_conteneur                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│   GOUVERNANCE                          REPORTING                  │
│   HashiCorp Vault (secrets)            PostgreSQL Gold -> Power BI │
│   Rôles + RLS + vues sécurisées        (filtré par workspace)     │
│   PostgreSQL Audit (traçabilité)                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Deux domaines métier

Les sources ne se lient pas naturellement entre elles. Le projet assume deux domaines séparés, reflétant des systèmes réels distincts.

| Domaine | Sources | Granularité | Faits |
|---|---|---|---|
| Commerce extérieur | Douanes + Comtrade | Statistiques agrégées | flux_commercial, importation, exportation, recettes_douane |
| Logistique portuaire | AIS | Mouvements de navires | mouvement_conteneur |

La dimension `dim_date` est partagée entre les deux domaines.

---

## Stack technique

| Outil | Rôle |
|---|---|
| Docker / Docker Compose | Conteneurisation de l'ensemble des services |
| Apache Airflow | Orchestration des pipelines |
| MinIO | Data Lake compatible S3 (Bronze / Silver / Gold) |
| PostgreSQL | Staging temporaire + Data Warehouse Gold + Audit |
| dbt Core | Transformations SQL et tests qualité |
| HashiCorp Vault | Gestion centralisée des secrets |
| Python | Logique d'ingestion et export Parquet |

---

## Sources de données

| Source | Format | Description | Volume |
|---|---|---|---|
| Douanes françaises (data.gouv.fr) | CSV | Déclarations import/export | ~5 M lignes |
| UN Comtrade API | JSON | Flux commerciaux internationaux | ~146 K lignes |
| AIS (simulé) | JSON | Mouvements de navires aux ports français | 15 K + incréments |
| Clients (simulé) | JSON | Importateurs / exportateurs français | 200 |

---

## Stratégies d'ingestion

Le choix de la stratégie est adapté à la nature de chaque source.

| Source | Nature | Stratégie | Justification |
|---|---|---|---|
| Douanes | Snapshot republié | Full-refresh (swap atomique dbt) | La source est la vérité complète ; capture les révisions |
| Comtrade | Snapshot par année | Full-refresh | Données figées une fois l'année close |
| AIS | Flux d'événements | Incrémental (merge par clé hashée) | Chaque mouvement est unique et immuable |

L'ingestion AIS illustre le pattern incrémental : matérialisation dbt `incremental`, clé de déduplication `HASHTEXT(mmsi || timestamp || port)`, et dimensions en *Slowly Changing Dimension* (ajout des nouveaux navires/ports sans recréation). Le pipeline Comtrade détecte dynamiquement les années à charger en interrogeant la couche Gold permanente (`fact_flux_commercial`), sans dépendre des tables de staging temporaires.

---

## Modèle dimensionnel (Gold)

### Dimensions

| Table | Lignes | Description |
|---|---|---|
| dim_date | 2 557 | Calendrier 2020-2026, libellés français |
| dim_pays | 240 | Codes ISO2 |
| dim_marchandise | 9 671 | Codes HS6 / HS4 / HS2 |
| dim_client | 200 | Importateurs / exportateurs |
| dim_navire | 850 | Navires (MMSI, IMO) |
| dim_port | 5 | Ports français |

### Faits

| Table | Lignes | Domaine |
|---|---|---|
| fact_flux_commercial | 5 082 641 | Commerce |
| fact_importation | 2 032 782 | Commerce |
| fact_exportation | 3 049 859 | Commerce |
| fact_recettes_douane | 2 032 782 | Commerce (lié à dim_client) |
| fact_mouvement_conteneur | 15 000+ | Logistique (incrémental) |

Cohérence vérifiée : `fact_importation + fact_exportation = fact_flux_commercial`.

---

## Sécurité & gouvernance

### Gestion des secrets applicatifs (HashiCorp Vault)

Aucun identifiant n'est codé en dur dans le code. Les credentials (PostgreSQL, MinIO, clé API Comtrade) sont stockés dans Vault et récupérés au runtime par les DAGs via un module utilitaire `utils_vault.py`.

```
Vault (KV v2)
├── secret/postgres   (username, password, host, port, database)
├── secret/minio      (access_key, secret_key, endpoint)
└── secret/comtrade   (api_key)
```

Les DAGs lisent les secrets via une connexion Airflow `vault_default` (type HashiCorp Vault). Les transformations dbt, exécutées dans des conteneurs éphémères via `DockerOperator`, reçoivent elles aussi leurs identifiants depuis Vault (injectés en variables d'environnement et lus par `profiles.yml` via `env_var()`). Toute la chaîne : DAGs Python et conteneurs dbt : partage ainsi une source unique de secrets.

**Mode développement** : Vault tourne en mode `-dev` (stockage en mémoire, token racine paramétrable). Les secrets sont perdus à chaque redémarrage de Docker et recréés via `vault_init.ps1` (Windows) ou `vault_init.sh` (Linux/CI), qui lisent leurs valeurs depuis `.env` pour garantir la cohérence.

**Évolution production** : mode serveur avec stockage persistant chiffré, unseal manuel, et `database secrets engine` pour la rotation automatique des credentials PostgreSQL (génération de comptes temporaires à TTL).

### Configuration de l'infrastructure (.env)

Les paramètres d'infrastructure (mots de passe PostgreSQL, clés de chiffrement Airflow, identifiants MinIO, token Vault) sont externalisés dans un fichier `.env` non versionné. Le dépôt fournit un modèle `.env.example` documentant chaque variable et les commandes de génération des clés sensibles. Le `docker-compose.yml` est entièrement paramétré par variables : aucune valeur sensible n'y figure en clair.

### Rôles et permissions

La gouvernance repose sur des rôles groupes (profils de permissions sans login) et des comptes techniques nominatifs qui en héritent. Les permissions sont définies une seule fois sur le rôle.

| Rôle groupe | Périmètre | Comptes membres |
|---|---|---|
| role_pipeline | INSERT/UPDATE/SELECT sur bronze, silver, gold, audit | user_airflow |
| role_data | SELECT sur silver, gold, audit | user_monitoring, user_data |
| role_powerbi | SELECT sur gold (filtré par RLS) | user_pbi_service |

Les comptes techniques et leurs mots de passe sont créés par le script `scripts/setup_roles.ps1` (ou `.sh`), qui lit les valeurs depuis `.env`. Les scripts SQL de gouvernance (`03` à `06`) sont idempotents et rejouables.

### Row Level Security (RLS)

La RLS est **active et forcée** sur les quatre tables de faits Gold. Elle cloisonne les données par workspace métier via la variable de session `app.workspace`.

| Domaine | Tables | Workspaces autorisés |
|---|---|---|
| Commerce | fact_importation, fact_exportation, fact_recettes_douane | DOUANE, DIRECTION |
| Logistique | fact_mouvement_conteneur | PORT, DIRECTION |

Le rôle `role_pipeline` conserve un accès complet (ingestion), `role_data` un accès en lecture complète, et `role_powerbi` un accès filtré par workspace. Exemple de comportement vérifié sur `fact_mouvement_conteneur` avec le compte Power BI :

```
Sans workspace défini   -> 0 ligne     (accès bloqué)
SET app.workspace='PORT'   -> N lignes  (workspace autorisé)
SET app.workspace='DOUANE' -> 0 ligne   (workspace non autorisé)
```

### Vues sécurisées

La vue `gold.v_dim_client_data` expose les attributs descriptifs des clients tout en masquant les colonnes sensibles (`numero_agrement_douane`, `chiffre_affaires_million_euro`). L'accès direct à la table `dim_client` est révoqué pour les rôles Data et Power BI, qui passent obligatoirement par la vue.

### Audit & traçabilité

| Table | Contenu |
|---|---|
| audit.audit_pipelines | Log des exécutions de DAG |
| audit.audit_fichiers_minio | Traçabilité des fichiers déposés dans MinIO |
| audit.logs_erreurs | Réservé à la gestion d'erreurs |
| audit.audit_acces | Réservé au suivi des accès |

---

## Qualité des données

Les transformations dbt embarquent plus de 25 tests automatisés : unicité des clés, contraintes `not_null`, et valeurs acceptées (`accepted_values` sur les sens de flux IMPORT/EXPORT). Les tests s'exécutent dans chaque pipeline avant l'export et le nettoyage des tables temporaires.

---

## Résultats

> Cette section contient les captures démontrant les résultats du pipeline.



---

## Installation & lancement

### Prérequis

- Docker Desktop avec intégration WSL2 (Windows) ou Docker Engine (Linux)
- 8 Go de RAM disponibles minimum

### Démarrage

```bash
# 1. Copier le modèle d'environnement et le remplir
cp .env.example .env
#    Generer les cles sensibles (Fernet, token Vault, mots de passe) :
#    les commandes sont documentées dans .env.example

# 2. Lancer la stack
docker compose up -d

# 3. Initialiser les secrets Vault (a relancer après chaque redémarrage)
#    Windows
.\vault\vault_init.ps1
#    Linux / Mac
bash vault/vault_init.sh

# 4. Appliquer la couche de gouvernance (rôles, RLS, vues)
#    Windows
.\scripts\setup_roles.ps1
#    Linux / Mac
bash scripts/setup_roles.sh

# 5. Activer les DAGs depuis l'UI (http://localhost:8080)
```

> **Note** : Vault tourne en mode développement (stockage en mémoire). Ses secrets sont perdus à chaque redémarrage de Docker : relancer `vault_init` pour les recréer. Les DAGs restent visibles dans l'interface même si Vault est temporairement vide.

### Interfaces

| Service | URL | Authentification |
|---|---|---|
| Airflow | http://localhost:8080 | compte admin de démo (voir installation) |
| MinIO Console | http://localhost:9001 | identifiants définis dans `.env` |
| Vault | http://localhost:8200 | token défini dans `.env` (VAULT_ROOT_TOKEN) |
| dbt docs | http://localhost:8081 | - |

---

## Évolutions futures

- **AIS temps réel** : remplacer la simulation batch par un flux streaming (Kafka / Spark Streaming) avec partitionnement temporel et stratégie de rétention.
- **Rotation automatique des secrets** : activer le `database secrets engine` de Vault.
- **fact_flux_international** : exploiter les données Comtrade aujourd'hui arrêtées en couche Silver.
- **Intégration RLS / Power BI** : propager `app.workspace` depuis Power BI vers PostgreSQL pour un cloisonnement de bout en bout.

---

## Auteur

Debassane K.
Data & BI
debassanek@gmail.com