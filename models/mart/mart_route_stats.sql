WITH base AS (
    SELECT
        origin,
        dest,
        tail_number,
        airline,
        actual_elapsed_time,
        arr_delay,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}
),
route_stats AS (
    SELECT
        origin,
        dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,
        round(AVG(actual_elapsed_time),2) AS avg_actual_elapsed_time,
        AVG(arr_delay) AS avg_arr_delay,
        MAX(arr_delay) AS max_arr_delay,
        MIN(arr_delay) AS min_arr_delay,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted
    FROM base
    GROUP BY origin, dest
),
origin_info AS (
    SELECT
        faa AS origin,
        city AS origin_city,
        country AS origin_country,
        name AS origin_airport_name
    FROM {{ ref('prep_flights') }}
),
dest_info AS (
    SELECT
        faa AS dest,
        city AS dest_city,
        country AS dest_country,
        name AS dest_airport_name
    FROM {{ ref('prep_flights') }}
)
SELECT
    rs.origin,
    rs.dest,
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_actual_elapsed_time,
    rs.avg_arr_delay,
    rs.max_arr_delay,
    rs.min_arr_delay,
    rs.total_cancelled,
    rs.total_diverted,
    oi.origin_city,
    oi.origin_country,
    oi.origin_airport_name,
    di.dest_city,
    di.dest_country,
    di.dest_airport_name
FROM route_stats rs
LEFT JOIN origin_info oi USING (origin)
LEFT JOIN dest_info di USING (dest)
ORDER BY rs.total_flights DESC




