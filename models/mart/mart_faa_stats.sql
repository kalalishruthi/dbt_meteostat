WITH departures AS (
    SELECT
        origin AS faa,
        COUNT(DISTINCT dest) AS unique_departure_connections,
        COUNT(*) AS total_departure_flights,
        SUM(cancelled) AS canceled_departure_flights,
        SUM(diverted) AS diverted_departure_flights,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS actual_departure_flights
    FROM prep_flights
    GROUP BY origin
),
arrivals AS (
    SELECT
        dest AS faa,
        COUNT(DISTINCT origin) AS unique_arrival_connections,
        COUNT(*) AS total_arrival_flights,
        SUM(cancelled) AS canceled_arrival_flights,
        SUM(diverted) AS diverted_arrival_flights,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS actual_arrival_flights
    FROM prep_flights
    GROUP BY dest
)
SELECT
    pa.faa,
    pa.name,
    pa.city,
    pa.country,
    COALESCE(d.unique_departure_connections, 0) AS unique_departure_connections,
    COALESCE(a.unique_arrival_connections, 0) AS unique_arrival_connections,
    COALESCE(d.total_departure_flights, 0) + COALESCE(a.total_arrival_flights, 0) AS total_planned_flights,
    COALESCE(d.canceled_departure_flights, 0) + COALESCE(a.canceled_arrival_flights, 0) AS total_canceled_flights,
    COALESCE(d.diverted_departure_flights, 0) + COALESCE(a.diverted_arrival_flights, 0) AS total_diverted_flights,
    COALESCE(d.actual_departure_flights, 0) + COALESCE(a.actual_arrival_flights, 0) AS total_actual_flights
FROM prep_airports pa
LEFT JOIN departures d ON pa.faa = d.faa
LEFT JOIN arrivals a ON pa.faa = a.faa
ORDER BY pa.faa;

















