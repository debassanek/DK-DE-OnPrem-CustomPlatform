{{ config(
    materialized='incremental',
    unique_key='id_mouvement_conteneur'
) }}

SELECT
    ABS(HASHTEXT(
        m.mmsi || m.timestamp_position::TEXT || m.port_destination
    ))::BIGINT                                  AS id_mouvement_conteneur,

    d.id_date,
    p.id_port,
    n.id_navire,
    m.mmsi,
    m.nom_navire,
    m.vitesse_noeuds,
    m.statut_navigation,
    m.port_destination,
    m.timestamp_position,
    m.ligne_valide

FROM {{ ref('mouvements_navires') }} m
LEFT JOIN {{ ref('dim_date') }}   d ON d.date_complete = m.timestamp_position::DATE
LEFT JOIN {{ ref('dim_port') }}   p ON p.code_locode   = m.port_destination
LEFT JOIN {{ ref('dim_navire') }} n ON n.mmsi           = m.mmsi

{% if is_incremental() %}
  WHERE m.timestamp_position > (SELECT MAX(timestamp_position) FROM {{ this }})
{% endif %}