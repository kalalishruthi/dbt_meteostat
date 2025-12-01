SELECT
  w.airport_code AS faa,
  w.date,
  COUNT(DISTINCT CASE WHEN f.origin = w.airport_code THEN f.dest END) AS unique_departures,
  COUNT(DISTINCT CASE WHEN f.dest   = w.airport_code THEN f.origin END) AS unique_arrivals,
  COUNT(*) AS planned,
  SUM(f.cancelled) AS cancelled,
  SUM(f.diverted) AS diverted,
  COUNT(f.arr_time) AS actual,
  w.min_temp_c,
  w.max_temp_c,
  w.precipitation_mm,
  w.max_snow_mm,
  w.avg_wind_direction,
  w.avg_wind_speed_kmh,
  w.wind_peakgust_kmh
FROM {{ ref('prep_weather_daily') }} w
LEFT JOIN {{ ref('prep_flights') }} f
  ON f.flight_date = w.date
 AND (f.origin = w.airport_code OR f.dest = w.airport_code)
GROUP BY
  w.airport_code, w.date,
  w.min_temp_c, w.max_temp_c, w.precipitation_mm, w.max_snow_mm,
  w.avg_wind_direction, w.avg_wind_speed_kmh, w.wind_peakgust_kmh
ORDER BY w.airport_code, w.date








