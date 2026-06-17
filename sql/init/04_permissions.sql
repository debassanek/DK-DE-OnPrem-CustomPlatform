-- =================================================================
-- DK-DE-OnPrem-CustomPlatform
-- Script : 04_permissions.sql
-- Description : Gestion des permissions
-- =================================================================

-- -----------------------------------------------------------------
-- SECTION 1 : Accès aux schémas (USAGE)
-- -----------------------------------------------------------------

-- Gold : accès à tous les rôles groupes
GRANT USAGE ON SCHEMA gold TO role_pipeline, role_powerbi, role_data;

-- Silver : accès aux roles groupes pipeline et data uniquement
GRANT USAGE ON SCHEMA silver TO role_pipeline, role_data;

-- Bronze : accès au role groupe pipeline uniquement
GRANT USAGE ON SCHEMA bronze TO role_pipeline;

-- Audit : accès au role groupe pipeline et data
GRANT USAGE ON SCHEMA audit TO role_pipeline, role_data;

-- -----------------------------------------------------------------
-- SECTION 2 : Permissions d'accès au service pipeline
-- -----------------------------------------------------------------

-- Accès lecture, modification et insertion sur les tables d'ingestion et de transformation
GRANT INSERT, UPDATE, SELECT
    ON ALL TABLES IN SCHEMA bronze TO role_pipeline;
GRANT INSERT, UPDATE, SELECT
    ON ALL TABLES IN SCHEMA silver TO role_pipeline;
GRANT INSERT, UPDATE, SELECT
    ON ALL TABLES IN SCHEMA gold   TO role_pipeline;
GRANT INSERT, UPDATE, SELECT
    ON ALL TABLES IN SCHEMA audit   TO role_pipeline;

-- -----------------------------------------------------------------
-- SECTION 3 : Permissions d'accès au service powerbi
-- -----------------------------------------------------------------

-- Accès lecture seule sur la couche Gold
GRANT SELECT
    ON ALL TABLES IN SCHEMA gold   TO role_powerbi;


-- -----------------------------------------------------------------
-- SECTION 4 : Permissions d'accès au data
-- -----------------------------------------------------------------

-- Accès lecture seule sur la couche Gold
GRANT SELECT
    ON ALL TABLES IN SCHEMA gold   TO role_data;
GRANT SELECT
    ON ALL TABLES IN SCHEMA silver   TO role_data;
GRANT SELECT
    ON ALL TABLES IN SCHEMA audit   TO role_data;

-- -----------------------------------------------------------------
-- SECTION 5 : Permissions sur les futures tables
-- -----------------------------------------------------------------

-- Gold
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT INSERT, UPDATE, SELECT ON TABLES TO role_pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT ON TABLES TO role_powerbi, role_data;

-- Silver
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT INSERT, UPDATE, SELECT ON TABLES TO role_pipeline; 
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT ON TABLES TO role_data;

-- Bronze
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT INSERT, UPDATE, SELECT ON TABLES TO role_pipeline;

-- Audit
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT INSERT, UPDATE, SELECT ON TABLES TO role_pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT SELECT ON TABLES TO role_data;
