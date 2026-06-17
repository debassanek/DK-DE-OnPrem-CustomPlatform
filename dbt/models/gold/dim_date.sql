
{{ config(materialized='table', schema = 'gold') }}

WITH dates AS (
    SELECT generate_series(
    '2020-01-01'::DATE,
    '2026-12-31'::DATE,
    '1 day'::INTERVAL
    )::DATE AS date_complete
)

SELECT 
    TO_CHAR(date_complete, 'YYYYMMDD')::INTEGER     AS id_date,
    date_complete,
    EXTRACT(DAY FROM date_complete)::SMALLINT       AS jour,
    EXTRACT(MONTH FROM date_complete)::SMALLINT     AS mois,
    EXTRACT(QUARTER FROM date_complete)::SMALLINT   AS trimestre,
    EXTRACT(YEAR FROM date_complete)::SMALLINT      AS annee,
    EXTRACT(WEEK FROM date_complete)::SMALLINT      AS semaine,
    CASE EXTRACT(DOW FROM date_complete)::INTEGER
        WHEN 0 THEN 'Dimanche'
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
    END                                AS nom_jour,

    CASE EXTRACT(MONTH FROM date_complete)::INTEGER
        WHEN 1  THEN 'Janvier'
        WHEN 2  THEN 'Février'
        WHEN 3  THEN 'Mars'
        WHEN 4  THEN 'Avril'
        WHEN 5  THEN 'Mai'
        WHEN 6  THEN 'Juin'
        WHEN 7  THEN 'Juillet'
        WHEN 8  THEN 'Août'
        WHEN 9  THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Décembre'
    END                                AS nom_mois,
    EXTRACT(DOW FROM date_complete) IN (0,6)        AS est_weekend 
FROM dates