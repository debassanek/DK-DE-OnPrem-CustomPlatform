-- =================================================================
-- DK-DE-OnPrem-CustomPlatform
-- Script     : 03_roles_securite.sql
-- Description: Gestion des rôles et utilisateurs (idempotent)
--
-- ARCHITECTURE :
--   Rôles groupes : profils de permissions (NOLOGIN)
--   Comptes nominatifs : un service = un compte
--   Héritage : chaque compte hérite d'un rôle groupe
--
-- IDEMPOTENCE :
--   DROP OWNED ... CASCADE retire les privilèges, politiques RLS
--   et vues liés aux rôles AVANT de les supprimer, puis CREATE.
--   Le script est rejouable. La séquence setup_roles (03->04->05->06)
--   reconstruit ensuite permissions, RLS et vues.
--
-- PRÉREQUIS : variables AIRFLOW_PASSWORD, PBI_SERVICE_PASSWORD,
--             MONITORING_PASSWORD, DATA_ACCESS_PASSWORD (psql -v)
-- EXÉCUTION : bash scripts/setup_roles.sh (ou setup_roles.ps1)
-- =================================================================

-- -----------------------------------------------------------------
-- PARTIE 0 : Nettoyage idempotent (gère les dépendances)
-- DROP OWNED retire tous les objets/privilèges liés au rôle
-- (permissions, politiques RLS, droits sur vues) AVANT le DROP ROLE
-- -----------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_pipeline') THEN
        EXECUTE 'DROP OWNED BY role_pipeline CASCADE';
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_powerbi') THEN
        EXECUTE 'DROP OWNED BY role_powerbi CASCADE';
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_data') THEN
        EXECUTE 'DROP OWNED BY role_data CASCADE';
    END IF;
END
$$;

-- Comptes (membres) d'abord, puis rôles groupes
DROP USER IF EXISTS user_airflow;
DROP USER IF EXISTS user_pbi_service;
DROP USER IF EXISTS user_monitoring;
DROP USER IF EXISTS user_data;

DROP ROLE IF EXISTS role_pipeline;
DROP ROLE IF EXISTS role_powerbi;
DROP ROLE IF EXISTS role_data;

-- -----------------------------------------------------------------
-- PARTIE 1 : Rôles groupes
-- Les permissions sont définies UNE fois sur le rôle ;
-- tous les comptes membres en héritent
-- -----------------------------------------------------------------

-- Rôle technique pipeline : INSERT/UPDATE/SELECT sur bronze, silver, gold, audit
CREATE ROLE role_pipeline NOLOGIN;

-- Rôle Power BI : SELECT sur gold uniquement
CREATE ROLE role_powerbi NOLOGIN;

-- Rôle data : SELECT sur silver, gold, audit
CREATE ROLE role_data NOLOGIN;

-- -----------------------------------------------------------------
-- PARTIE 2 : Comptes techniques
-- Mots de passe injectés via psql -v (substitution :'VARIABLE')
-- -----------------------------------------------------------------

-- Airflow : ingestion des données (tous les DAGs)
CREATE USER user_airflow
    WITH PASSWORD :'AIRFLOW_PASSWORD'
    INHERIT
    CONNECTION LIMIT 10;

-- Power BI : service de reporting connecté au workspace
CREATE USER user_pbi_service
    WITH PASSWORD :'PBI_SERVICE_PASSWORD'
    INHERIT
    CONNECTION LIMIT 20;

-- Monitoring / supervision (Grafana, alertes)
CREATE USER user_monitoring
    WITH PASSWORD :'MONITORING_PASSWORD'
    INHERIT
    CONNECTION LIMIT 5;

-- Accès direct en lecture (DBeaver, psql)
CREATE USER user_data
    WITH PASSWORD :'DATA_ACCESS_PASSWORD'
    INHERIT
    CONNECTION LIMIT 5;

-- -----------------------------------------------------------------
-- PARTIE 3 : Attribution des rôles groupes aux comptes
-- -----------------------------------------------------------------

GRANT role_pipeline TO user_airflow;
GRANT role_powerbi  TO user_pbi_service;
GRANT role_data     TO user_monitoring;
GRANT role_data     TO user_data;