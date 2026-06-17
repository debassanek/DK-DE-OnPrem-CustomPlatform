-- =================================================================
-- DK-DE-OnPrem-CustomPlatform
-- Script : 05_rls.sql
-- Description : Row Level Security sur les tables de faits Gold (idempotent)
--               DROP POLICY IF EXISTS avant chaque CREATE -> rejouable
-- =================================================================

-- -----------------------------------------------------------------
-- SECTION 1 : Activation de la RLS sur les tables
-- -----------------------------------------------------------------

ALTER TABLE gold.fact_mouvement_conteneur ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_mouvement_conteneur FORCE  ROW LEVEL SECURITY;

ALTER TABLE gold.fact_importation ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_importation FORCE  ROW LEVEL SECURITY;

ALTER TABLE gold.fact_exportation ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_exportation FORCE  ROW LEVEL SECURITY;

ALTER TABLE gold.fact_recettes_douane ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_recettes_douane FORCE  ROW LEVEL SECURITY;

-- -----------------------------------------------------------------
-- SECTION 2 : Politiques fact_importation
-- -----------------------------------------------------------------

DROP POLICY IF EXISTS policy_import_pipeline ON gold.fact_importation;
DROP POLICY IF EXISTS policy_import_data     ON gold.fact_importation;
DROP POLICY IF EXISTS policy_import_powerbi  ON gold.fact_importation;

-- Pipeline : accès complet sans restriction
CREATE POLICY policy_import_pipeline
    ON gold.fact_importation
    FOR ALL TO role_pipeline
    USING (TRUE)
    WITH CHECK (TRUE);

-- Data : lecture complète
CREATE POLICY policy_import_data
    ON gold.fact_importation
    FOR SELECT TO role_data
    USING (TRUE);

-- Power BI : lecture filtrée par workspace
CREATE POLICY policy_import_powerbi
    ON gold.fact_importation
    FOR SELECT TO role_powerbi
    USING (
        current_setting('app.workspace', TRUE)
            IN ('DOUANE', 'DIRECTION')
    );

-- -----------------------------------------------------------------
-- SECTION 3 : Politiques fact_exportation
-- -----------------------------------------------------------------

DROP POLICY IF EXISTS policy_export_pipeline ON gold.fact_exportation;
DROP POLICY IF EXISTS policy_export_data     ON gold.fact_exportation;
DROP POLICY IF EXISTS policy_export_powerbi  ON gold.fact_exportation;

-- Pipeline : accès complet sans restriction
CREATE POLICY policy_export_pipeline
    ON gold.fact_exportation
    FOR ALL TO role_pipeline
    USING (TRUE)
    WITH CHECK (TRUE);

-- Data : lecture complète
CREATE POLICY policy_export_data
    ON gold.fact_exportation
    FOR SELECT TO role_data
    USING (TRUE);

-- Power BI : lecture filtrée par workspace
CREATE POLICY policy_export_powerbi
    ON gold.fact_exportation
    FOR SELECT TO role_powerbi
    USING (
        current_setting('app.workspace', TRUE)
            IN ('DOUANE', 'DIRECTION')
    );

-- -----------------------------------------------------------------
-- SECTION 4 : Politiques fact_recettes_douane
-- -----------------------------------------------------------------

DROP POLICY IF EXISTS policy_recettes_pipeline ON gold.fact_recettes_douane;
DROP POLICY IF EXISTS policy_recettes_data     ON gold.fact_recettes_douane;
DROP POLICY IF EXISTS policy_recettes_powerbi  ON gold.fact_recettes_douane;

-- Pipeline : accès complet sans restriction
CREATE POLICY policy_recettes_pipeline
    ON gold.fact_recettes_douane
    FOR ALL TO role_pipeline
    USING (TRUE)
    WITH CHECK (TRUE);

-- Data : lecture complète
CREATE POLICY policy_recettes_data
    ON gold.fact_recettes_douane
    FOR SELECT TO role_data
    USING (TRUE);

-- Power BI : lecture filtrée par workspace
CREATE POLICY policy_recettes_powerbi
    ON gold.fact_recettes_douane
    FOR SELECT TO role_powerbi
    USING (
        current_setting('app.workspace', TRUE)
            IN ('DOUANE', 'DIRECTION')
    );

-- -----------------------------------------------------------------
-- SECTION 5 : Politiques fact_mouvement_conteneur
-- -----------------------------------------------------------------

DROP POLICY IF EXISTS policy_mouvement_pipeline ON gold.fact_mouvement_conteneur;
DROP POLICY IF EXISTS policy_mouvement_data     ON gold.fact_mouvement_conteneur;
DROP POLICY IF EXISTS policy_mouvement_powerbi  ON gold.fact_mouvement_conteneur;

-- Pipeline : accès complet sans restriction
CREATE POLICY policy_mouvement_pipeline
    ON gold.fact_mouvement_conteneur
    FOR ALL TO role_pipeline
    USING (TRUE)
    WITH CHECK (TRUE);

-- Data : lecture complète
CREATE POLICY policy_mouvement_data
    ON gold.fact_mouvement_conteneur
    FOR SELECT TO role_data
    USING (TRUE);

-- Power BI : lecture filtrée par workspace
CREATE POLICY policy_mouvement_powerbi
    ON gold.fact_mouvement_conteneur
    FOR SELECT TO role_powerbi
    USING (
        current_setting('app.workspace', TRUE)
            IN ('PORT', 'DIRECTION')
    );