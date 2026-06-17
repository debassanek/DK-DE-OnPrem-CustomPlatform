-- =================================================================
-- DK-DE-OnPrem-CustomPlatform
-- Script : 06_vues_securisees.sql
-- Description : Vues sécurisées pour les rôles Data et Power BI (idempotent)
--               La vue masque les colonnes sensibles de dim_client :
--                 - numero_agrement_douane (identifiant douanier)
--                 - chiffre_affaires_million_euro (donnée financière)
-- =================================================================

-- -----------------------------------------------------------------
-- VUE 1 : gold.v_dim_client_data
-- Expose dim_client SANS les colonnes sensibles
-- -----------------------------------------------------------------

CREATE OR REPLACE VIEW gold.v_dim_client_data AS
    SELECT
        id_client,
        code_client,
        raison_sociale,
        type_client,
        code_pays_iso2,
        ville,
        secteur_activite,
        est_actif
    FROM gold.dim_client
;

-- Accès en lecture à la vue pour Data et Power BI
GRANT SELECT ON gold.v_dim_client_data TO role_data;
GRANT SELECT ON gold.v_dim_client_data TO role_powerbi;

-- Révocation de l'accès direct à la table
-- (les colonnes sensibles ne sont accessibles qu'aux rôles autorisés)
REVOKE SELECT ON gold.dim_client FROM role_data;
REVOKE SELECT ON gold.dim_client FROM role_powerbi;