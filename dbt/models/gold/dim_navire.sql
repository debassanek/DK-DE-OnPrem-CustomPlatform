{{ config(
    materialized='incremental',
    unique_key='mmsi'
) }}

WITH navires AS (
    SELECT DISTINCT ON (mmsi)
        mmsi,
        imo,
        nom_navire
    FROM {{ ref('mouvements_navires') }}
    WHERE mmsi IS NOT NULL

    {% if is_incremental() %}
      -- Uniquement les navires pas encore dans la dimension
      AND mmsi NOT IN (SELECT mmsi FROM {{ this }})
    {% endif %}

    ORDER BY mmsi, date_ingestion DESC
)

SELECT
    ABS(HASHTEXT(mmsi))::INTEGER  AS id_navire,
    mmsi,
    imo,
    nom_navire
FROM navires