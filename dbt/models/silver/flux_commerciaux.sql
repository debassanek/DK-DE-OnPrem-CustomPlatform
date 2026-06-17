{{ config(materialized='table', schema='silver') }}

SELECT
    id_raw                                      AS id_source,
    source_systeme,
    source_fichier,
    date_ingestion,

-- Normalisation du sens du flux
    CASE sens_flux
        WHEN 'X' THEN 'EXPORT'
        WHEN 'M' THEN 'IMPORT'
        ELSE 'INCONNU'
    END                                         AS sens_flux,

    -- Periode
    TRIM(periode)                             AS periode,

    -- Normalisation du code pays
    UPPER(TRIM(code_pays_exportateur))                   AS code_pays_reporter,
    UPPER(TRIM(code_pays_importateur))                   AS code_pays_partner,

    -- Normalisation du code marchandise
    TRIM(code_hs_marchandise)                   AS code_hs_marchandise,

    -- Cast des mesures
    NULLIF(TRIM(valeur_usd_brute::TEXT), '')::NUMERIC     AS valeur_usd,
    NULLIF(TRIM(poids_kg_brut::TEXT), '')::NUMERIC   AS poids_kg,

    -- Validation de la ligne
    CASE
        WHEN valeur_usd_brute IS NULL THEN FALSE
        ELSE TRUE
    END                                         AS ligne_valide

FROM {{ source('bronze', 'stg_flux_commerciaux') }}
WHERE sens_flux IN ('X', 'M')
