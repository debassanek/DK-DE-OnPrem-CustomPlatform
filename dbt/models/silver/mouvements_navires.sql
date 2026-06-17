{{ config(materialized='table', schema='silver') }}

SELECT
    id_raw                                              AS id_source,
    source_systeme,
    source_fichier,
    date_ingestion,

    TRIM(numero_mmsi)                                   AS mmsi,
    TRIM(numero_imo)                                    AS imo,
    UPPER(TRIM(nom_navire))                             AS nom_navire,
    UPPER(TRIM(statut_navigation))                      AS statut_navigation,

    NULLIF(TRIM(latitude_brute), '')::NUMERIC(9,6)      AS latitude,
    NULLIF(TRIM(longitude_brute), '')::NUMERIC(9,6)     AS longitude,
    NULLIF(TRIM(vitesse_noeud), '')::NUMERIC(5,1)       AS vitesse_noeuds,
    UPPER(TRIM(port_destination))                       AS port_destination,
    NULLIF(TRIM(temps_position), '')::TIMESTAMP         AS timestamp_position,

    CASE
        WHEN numero_mmsi IS NULL THEN FALSE
        WHEN temps_position IS NULL THEN FALSE
        ELSE TRUE
    END                                                 AS ligne_valide

FROM {{ source('bronze', 'stg_mouvements_navires') }}
WHERE numero_mmsi IS NOT NULL