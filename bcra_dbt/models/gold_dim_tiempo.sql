{{ config(materialized='table') }}

SELECT DISTINCT
    fecha,
    date_part(year, fecha) AS anio,
    date_part(month, fecha) AS mes,
    date_part(day, fecha) AS dia,
    to_char(fecha, 'YYYY-MM') AS anio_mes
FROM {{ ref('silver_monetarias') }}