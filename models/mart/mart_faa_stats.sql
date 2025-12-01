WITH departures AS ( 
    SELECT 
        origin AS faa,
        COUNT(DISTINCT dest) AS nunique_to,
        COUNT(*) AS dep_planned,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS dep_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS dep_diverted,
        COUNT(
            CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 END
        ) AS dep_actual
    FROM {{ ref('prep_flights') }}
    GROUP BY origin
),
arrivals AS (
    SELECT 
        dest AS faa,
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(*) AS arr_planned,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS arr_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS arr_diverted,
        COUNT(
            CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 END
        ) AS arr_actual
    FROM {{ ref('prep_flights') }}
    GROUP BY dest
),
total_stats AS (
    SELECT 
        d.faa,
        d.nunique_to,
        a.nunique_from,
        d.dep_planned + a.arr_planned AS total_planned,
        d.dep_cancelled + a.arr_cancelled AS total_cancelled,
        d.dep_diverted + a.arr_diverted AS total_diverted,
        d.dep_actual + a.arr_actual AS total_actual
    FROM departures d
    JOIN arrivals a USING (faa)
)
SELECT 
    ap.city,
    ap.country,
    ap.name AS airport_name,
    t.*
FROM total_stats t
LEFT JOIN {{ ref('prep_airports') }} ap USING (faa)
ORDER BY total_diverted DESC;


















