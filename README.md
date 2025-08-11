# Proyecto de Ingeniería de Datos - Extracción automática de variables del BCRA

**Principales variables monetarias del BCRA (01/01/2020 a la actualidad)**

Este proyecto nace para resolver la necesidad de acceder de forma automatizada y estructurada a datos públicos del Banco Central de la República Argentina (BCRA), que suelen estar en formatos poco prácticos para análisis.  
Se desarrolló como parte de un proyecto integral de ingeniería de datos aplicando un pipeline ETL con datos reales del sistema financiero argentino.

El pipeline extrae variables históricas monetarias (inflación, tipo de cambio, tasas de interés, entre otras) desde la API del BCRA, las procesa y las carga en Snowflake para su posterior análisis y visualización.  
Se utiliza dbt para la transformación y modelado de datos, asegurando calidad y estructura con un modelo en estrella.  
Finalmente, Power BI consume los datos para dashboards interactivos que facilitan la toma de decisiones financieras.

Además, el pipeline está automatizado para ejecutarse periódicamente mediante Airflow y el Programador de Tareas de Windows, garantizando datos actualizados constantemente.

---

## Tecnologías utilizadas

- Python (extracción y carga)  
- Snowflake (almacenamiento de datos)  
- dbt (transformación y modelado de datos)  
- Apache Airflow (orquestación del pipeline)  
- Power BI (visualización y análisis)  

---

## Arquitectura y flujo del pipeline

### Extracción

- Se realiza la extracción automatizada de datos desde la API pública del BCRA mediante scripts Python.  
- Los datos extraídos incluyen múltiples variables monetarias históricas, actualizadas periódicamente.

### Carga

- Los datos son cargados en tablas `raw` (silver) en Snowflake.  
- Se implementa carga incremental para insertar únicamente datos nuevos según la última fecha disponible.

### Transformación y modelado

- dbt realiza la transformación, limpieza y modelado de los datos en Snowflake.  
- Se construye un modelo en estrella con tablas de dimensión (`dim_variable`, `dim_tiempo`) y tabla de hechos (`hechos_monetarios`).  


### Orquestación

- Apache Airflow gestiona la ejecución programada de cada etapa del pipeline: extracción, carga y transformación.  
- El pipeline está configurado para ejecutarse automáticamente según cronograma definido.

- ## Visualización con Power BI

- Power BI se conecta a Snowflake para consumir los datos modelados por dbt.  
- Los dashboards incluyen:  
  - Últimos valores de variables clave.  
  - Análisis de variaciones mensuales.  
  - Tendencias históricas y patrones.  
- Facilita la interpretación y análisis financiero para usuarios no técnicos.
