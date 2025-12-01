SELECT
    pf.origin,
    pa.city        AS origin_city,
    pa.country     AS origin_country,
    pa.name        AS origin_name,
    pf.dest,
    ps.city        AS dest_city,
    ps.country     AS dest_country,
    ps.name        AS dest_name,
    COUNT(*)                       AS number_of_flights,
    COUNT(DISTINCT pf.tail_number) AS number_of_unique_airplanes,
    COUNT(DISTINCT pf.airline)     AS number_of_unique_airlines,
    ROUND(AVG(pf.actual_elapsed_time), 2) AS average_actual_elapsed_time,
    AVG(pf.arr_delay)               AS average_arrival_delay,
    MAX(pf.arr_delay)               AS max_arrival_delay,
    MIN(pf.arr_delay)               AS min_arrival_delay,
    SUM(CASE WHEN pf.cancelled = 1 THEN 1 ELSE 0 END) AS number_of_canceled_flights,
    SUM(CASE WHEN pf.diverted  = 1 THEN 1 ELSE 0 END) AS total_number_of_diverted_flights
FROM {{ ref('prep_flights') }} pf
-- Join airport info for origin
JOIN {{ ref('prep_airports') }} pa
    ON pa.faa = pf.origin
-- Join airport info for destination
JOIN {{ ref('prep_airports') }} ps
    ON ps.faa = pf.dest
GROUP BY 
    pf.origin, pa.city, pa.country, pa.name,
    pf.dest, ps.city, ps.country, ps.name
ORDER BY pf.origin, pf.dest
