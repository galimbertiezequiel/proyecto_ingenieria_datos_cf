{{ config(
    materialized='incremental',
    unique_key=['variable_id', 'fecha']
) }}

WITH src AS (
  SELECT
    id AS variable_id,
    fecha,
    valor
  FROM {{ ref('silver_monetarias') }}
  WHERE valor IS NOT NULL
)

{% if not is_incremental() %}
  SELECT * FROM src
{% else %}
  MERGE INTO {{ this }} AS target
  USING src AS source
  ON target.variable_id = source.variable_id
    AND target.fecha = source.fecha
  WHEN MATCHED THEN UPDATE SET
    valor = source.valor
  WHEN NOT MATCHED THEN INSERT (variable_id, fecha, valor)
    VALUES (source.variable_id, source.fecha, source.valor)
;
{% endif %}