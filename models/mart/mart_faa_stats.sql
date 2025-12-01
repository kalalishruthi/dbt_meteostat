WITH departures AS (
    SELECT origin AS faa, COUNT(DISTINCT dest) AS unique_departures
    FROM {{ ref('prep_flights') }}
    GROUP BY origin
),
arrivals AS (
    SELECT dest AS faa, COUNT(DISTINCT origin) AS unique_arrivals
    FROM {{ ref('prep_flights') }}
    GROUP BY dest
),
flight_totals AS (
    SELECT 
        faa,
        COUNT(*) AS total_planned,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_canceled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS total_actual
    FROM (
        SELECT origin AS faa, cancelled, diverted FROM {{ ref('prep_flights') }}
        UNION ALL
        SELECT dest AS faa, cancelled, diverted FROM {{ ref('prep_flights') }}
    ) x
    GROUP BY faa
)
SELECT 
    pa.faa,
    pa.name,
    COALESCE(d.unique_departures, 0) AS unique_departure_connections,
    COALESCE(a.unique_arrivals, 0) AS unique_arrival_connections,
    COALESCE(t.total_planned, 0) AS total_planned_flights,
    COALESCE(t.total_canceled, 0) AS total_canceled_flights,
    COALESCE(t.total_diverted, 0) AS total_diverted_flights,
    COALESCE(t.total_actual, 0) AS total_actual_flights
FROM {{ ref('prep_airports') }} pa
LEFT JOIN departures d ON pa.faa = d.faa
LEFT JOIN arrivals   a ON pa.faa = a.faa
LEFT JOIN flight_totals t ON pa.faa = t.faa
ORDER BY pa.faa


















