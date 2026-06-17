
{{config(materialized='table', schema='silver')}}

SELECT
    id_raw                                  As id_source,
    source_systeme,
    source_fichier,
    date_ingestion,


    TRIM(siren)                             AS siren,
    TRIM(siret)                             AS siret,
    UPPER(TRIM(raison_sociale))             AS raison_sociale,
    UPPER(TRIM(secteur_activite))           AS secteur_activite,
    TRIM(ville)                             AS ville,
    UPPER(TRIM(code_pays))                  AS code_pays,
    NULLIF(TRIM(chiffre_affaires_million::TEXT), '')::NUMERIC(15, 2) AS chiffre_affaires_million,
    TRIM(numero_agrement_douane)            AS numero_agrement_douane,

    CASE
        WHEN siren IS NULL THEN FALSE
        ELSE TRUE
        END                                 AS ligne_valide

FROM {{source('bronze', 'stg_clients')}}
WHERE siren IS NOT NULL