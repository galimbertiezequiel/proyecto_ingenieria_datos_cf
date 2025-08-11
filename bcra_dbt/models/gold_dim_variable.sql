{{ config(materialized='table') }}

SELECT DISTINCT
    id AS variable_id,
    variable AS nombre_variable
FROM {{ ref('silver_monetarias') }}