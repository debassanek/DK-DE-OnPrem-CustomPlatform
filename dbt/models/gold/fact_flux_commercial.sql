

{{ config(materialized ='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY d.id_date, p.id_pays, m.id_marchandise)::BIGINT AS id_flux,
    -- Clés étrangères vers les dimensions
    d.id_date,
    p.id_pays,
    m.id_marchandise,

    --Mesures
    s.sens_flux,
    SUM(s.valeur_euro)  AS valeur_euro,
    SUM(s.poids_kg_net) AS poids_kg_net,
    COUNT(*)            AS nb_declarations
FROM {{ ref('declarations_douane') }} s

-- Jointures vers dimensions
LEFT JOIN {{ ref('dim_date') }}         d ON d.date_complete = s.date_declaration
LEFT JOIN {{ ref('dim_pays') }}         p ON p.code_iso2    = s.code_pays_destination
LEFT JOIN {{ ref('dim_marchandise') }}  m ON m.code_hs_marchandise  = s.code_hs_marchandise

GROUP BY
    d.id_date, p.id_pays, m.id_marchandise,
    s.sens_flux