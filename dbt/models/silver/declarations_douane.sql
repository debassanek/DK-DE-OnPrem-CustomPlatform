{{ config(materialized='table', schema='silver') }}

SELECT
    id_raw                                      AS id_source,
    source_systeme,
    source_fichier,
    date_ingestion,

    CASE sens_flux
        WHEN 'E' THEN 'EXPORT'
        WHEN 'I' THEN 'IMPORT'
        ELSE 'INCONNU'
    END                                         AS sens_flux,

    TO_DATE(
        annee_declaration || '-' ||
        LPAD(mois_declaration, 2, '0') || '-01',
        'YYYY-MM-DD'
    )                                           AS date_declaration,

    UPPER(TRIM(code_pays_destination))          AS code_pays_destination,
    TRIM(code_hs_marchandise)                   AS code_hs_marchandise,
    NULLIF(TRIM(valeur_brute), '')::NUMERIC     AS valeur_euro,
    NULLIF(TRIM(poids_brut), '')::NUMERIC       AS poids_kg_net,

    CASE
        WHEN valeur_brute IS NULL THEN FALSE
        WHEN poids_brut IS NULL   THEN FALSE
        ELSE TRUE
    END                                         AS ligne_valide

FROM {{ source('bronze', 'stg_declarations_douane') }}
WHERE sens_flux IN ('E', 'I')