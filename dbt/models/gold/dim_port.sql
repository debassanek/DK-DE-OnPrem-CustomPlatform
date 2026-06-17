{{ config(
    materialized='incremental',
    unique_key='code_locode'
) }}

WITH ports AS (
    SELECT DISTINCT port_destination AS code_locode
    FROM {{ ref('mouvements_navires') }}
    WHERE port_destination IS NOT NULL

    {% if is_incremental() %}
      AND port_destination NOT IN (SELECT code_locode FROM {{ this }})
    {% endif %}
)

SELECT
    ABS(HASHTEXT(code_locode))::INTEGER  AS id_port,
    code_locode,
    CASE code_locode
        WHEN 'FRMRS' THEN 'Marseille / Fos-sur-Mer'
        WHEN 'FRLEH' THEN 'Le Havre'
        WHEN 'FRURO' THEN 'Rouen'
        WHEN 'FRDKK' THEN 'Dunkerque'
        WHEN 'FRNTS' THEN 'Nantes / Saint-Nazaire'
        ELSE 'Inconnu'
    END                                   AS nom_port,
    'FR'                                  AS code_pays_iso2,
    'MARITIME'                            AS type_port
FROM ports