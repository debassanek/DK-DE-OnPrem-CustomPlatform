
{{ config(materialized='table', schema='gold')}}

SELECT
    ROW_NUMBER() OVER(ORDER BY s.date_declaration)::BIGINT AS id_exportation,
    
    --Clés étrangères
    d.id_date,
    p.id_pays,
    m.id_marchandise,

    --Mesures
    SUM(s.valeur_euro)          AS valeur_euro,
    SUM(s.poids_kg_net)         AS poids_kg_net,
    COUNT(*)                    AS nb_exportation
FROM
    {{ ref('declarations_douane') }} s
LEFT JOIN
    {{ ref('dim_date') }} d ON d.date_complete = s.date_declaration 
LEFT JOIN
    {{ ref('dim_pays') }} p ON p.code_iso2 = s.code_pays_destination
LEFT JOIN
    {{ ref('dim_marchandise') }} m ON  m.code_hs_marchandise = s.code_hs_marchandise
WHERE s.sens_flux ='EXPORT'

GROUP BY

    d.id_date,
    p.id_pays,
    m.id_marchandise ,
    s.date_declaration