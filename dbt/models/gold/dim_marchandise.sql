
{{ config(materialized='table', schema='gold') }}

WITH marchandises AS (
    SELECT DISTINCT code_hs_marchandise
    FROM {{ ref('declarations_douane') }}
    WHERE code_hs_marchandise IS NOT NULL

)

SELECT
    ROW_NUMBER() OVER (ORDER BY code_hs_marchandise)::INTEGER AS id_marchandise,
    code_hs_marchandise,
    LEFT(code_hs_marchandise, 4) AS code_hs4,
    LEFT(code_hs_marchandise, 2) AS code_hs2
FROM marchandises