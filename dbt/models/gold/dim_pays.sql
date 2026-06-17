
{{ config(materialized='table', schema='gold') }}

WITH pays_distincts AS (
    SELECT DISTINCT code_pays_destination AS code_iso2
    FROM {{ ref('declarations_douane') }}
    WHERE code_pays_destination IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY code_iso2)::INTEGER  AS id_pays,
    code_iso2
FROM pays_distincts
