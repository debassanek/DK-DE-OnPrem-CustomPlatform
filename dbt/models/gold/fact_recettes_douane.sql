{{ config(materialized='table', schema='gold') }}

WITH recettes AS (
    SELECT
        d.id_date,
        m.id_marchandise,
        p.id_pays,
        SUM(s.valeur_euro)                  AS valeur_imposable,
        ROUND(SUM(s.valeur_euro)*0.05, 2)   AS montant_droits_douane,
        ROUND(SUM(s.valeur_euro)*0.20, 2)   AS montant_tva,
        ROUND(SUM(s.valeur_euro)*0.25, 2)   AS montant_total_percu,
        COUNT(*)                            AS nb_declarations
    FROM {{ ref('declarations_douane') }} s
    LEFT JOIN {{ ref('dim_date') }}        d ON d.date_complete       = s.date_declaration
    LEFT JOIN {{ ref('dim_pays') }}        p ON p.code_iso2           = s.code_pays_destination
    LEFT JOIN {{ ref('dim_marchandise') }} m ON m.code_hs_marchandise = s.code_hs_marchandise
    WHERE s.sens_flux = 'IMPORT'
    GROUP BY d.id_date, m.id_marchandise, p.id_pays
),

recettes_numerotees AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY id_date) AS num_ligne,
        *
    FROM recettes
)

SELECT
    num_ligne::BIGINT                        AS id_recette,

    -- Client assigné via modulo sur 200 clients
    ((num_ligne % 200) + 1)::INTEGER         AS id_client,

    id_date,
    id_marchandise,
    id_pays,
    valeur_imposable,
    montant_droits_douane,
    montant_tva,
    montant_total_percu,
    nb_declarations

FROM recettes_numerotees