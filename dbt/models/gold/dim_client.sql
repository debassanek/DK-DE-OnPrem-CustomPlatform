{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY siren)::INTEGER  AS id_client,
    siren                                        AS code_client,
    raison_sociale,

    -- type_client basé sur secteur
    CASE
        WHEN secteur_activite LIKE '%IMPORT%' AND secteur_activite LIKE '%EXPORT%' THEN 'MIXTE'
        WHEN secteur_activite LIKE '%IMPORT%' THEN 'IMPORTATEUR'
        WHEN secteur_activite LIKE '%EXPORT%' THEN 'EXPORTATEUR'
        ELSE 'TRANSITAIRE'
    END                                          AS type_client,

    code_pays                                    AS code_pays_iso2,
    ville,
    secteur_activite,
    numero_agrement_douane,
    chiffre_affaires_million                     AS chiffre_affaires_million_euro,
    TRUE                                         AS est_actif

FROM {{ ref('clients') }}
WHERE ligne_valide = TRUE