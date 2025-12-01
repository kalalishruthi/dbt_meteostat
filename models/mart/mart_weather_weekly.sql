SELECT
  airport_code,
  date_trunc('week', date) AS week,
  AVG(avg_temp_c) as weekly_avg_temp,
  MIN(min_temp_c) AS weekly_min_temp,
  MAX(max_temp_c) AS weekly_max_temp,
  SUM(precipitation_mm) AS weekly_precipitation,
  MAX(max_snow_mm) AS weekly_max_snow,
  AVG(avg_wind_direction) AS weekly_avg_wind_direction,
  AVG(avg_wind_speed_kmh) AS weekly_avg_wind_speed,
  MAX(wind_peakgust_kmh) AS weekly_peak_gust,
  AVG(avg_pressure_hpa) AS avg_weekly_pressure_hpa, 
  SUM(sun_minutes) AS sum_weekly_sun_minutes
FROM {{ ref('prep_weather_daily') }}
GROUP BY airport_code, date_trunc('week', date)
ORDER BY airport_code, week;