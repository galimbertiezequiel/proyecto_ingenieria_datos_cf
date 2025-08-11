# Proyecto de Ingeniería de Datos con Airflow + Snowflake + DBT
# Adaptado desde el pipeline original que usaba MySQL

# %% LIBRERIAS
import requests
import pandas as pd
from datetime import datetime
import time
import unicodedata
import logging
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# %% LOGGING
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"pipeline_bcra_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

logging.info("Inicio de ejecución del pipeline BCRA")

# %% CONEXION A LA API
try:
    url = 'https://api.bcra.gob.ar/estadisticas/v3.0/monetarias'
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        data = response.json()
        variables_info = data['results']
        descripciones = {v['idVariable']: v['descripcion'] for v in variables_info}
    else:
        logging.warning("No se pudo obtener la lista de variables")
        descripciones = {}
except Exception as e:
    logging.error("Error al obtener la lista de variables", exc_info=True)
    descripciones = {}

# %% PARAMETROS DE EXTRACCION
ids = [1, 4, 5, 14, 15, 16, 27, 28, 32, 35]
fecha_desde = '2020-01-01'
fecha_hasta = datetime.now().strftime('%Y-%m-%d')
todas_las_series = []

# %% DESCARGA DE DATOS
for id_variable in ids:
    url = f'https://api.bcra.gob.ar/estadisticas/v3.0/monetarias/{id_variable}?desde={fecha_desde}&hasta={fecha_hasta}'
    try:
        response = requests.get(url, verify=False, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'results' in data and data['results']:
                df_variable = pd.json_normalize(data['results'])
                df_variable['fecha'] = pd.to_datetime(df_variable['fecha'])
                df_variable['descripcion'] = descripciones.get(id_variable, 'Desconocido')
                todas_las_series.append(df_variable)
                logging.info(f"Datos extraídos para variable ID {id_variable}")
    except Exception as e:
        logging.error(f"Error extrayendo datos de la variable {id_variable}", exc_info=True)

# %% CONSOLIDACION Y TRANSFORMACION
if todas_las_series:
    df_final = pd.concat(todas_las_series, ignore_index=True)
    df_final = df_final.sort_values(by=['idVariable', 'fecha'])
    df_final = df_final.rename(columns={'idVariable': 'id', 'descripcion': 'variable'})
    df_final = df_final[['id', 'variable', 'fecha', 'valor']]
    df_final['variable'] = df_final['variable'].apply(
        lambda texto: unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    )

#  Asegurar que la fecha sea tipo datetime64 para que Snowflake la interprete bien
    df_final['fecha'] = pd.to_datetime(df_final['fecha']).dt.date

    # Cambiar columnas a mayúsculas para Snowflake
    df_final.columns = [col.upper() for col in df_final.columns]

# %% CONEXION A SNOWFLAKE
try:
    conn = snowflake.connector.connect(
        user="USUARIO_SNOWFLAKE",
        password="CLAVE_SNOWFLAKE",
        account = 'GHHGFVF-GL16731',
        warehouse="INGEST_WAREHOUSE",
        database="BCRA_RAW",
        schema="PUBLIC"
    )
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_final,
        table_name="MONETARIAS_RAW",
        auto_create_table=False,
        overwrite=True
    )
    if success:
        logging.info(f"{nrows} registros insertados en Snowflake")
    else:
        logging.error("Falló la carga de datos en Snowflake")
    conn.close()
except Exception as e:
    logging.error("Error conectando a Snowflake", exc_info=True)

logging.info("Fin del pipeline BCRA")

# %%
df_final
# %%
print(df_final.dtypes)
# %%
import pandas as pd

try:
    print(pd.io.parquet.get_engine('fastparquet'))
except Exception as e:
    print(f"fastparquet error: {e}")

try:
    print(pd.io.parquet.get_engine('pyarrow'))
except Exception as e:
    print(f"pyarrow error: {e}")
# %%
import sys
print(sys.executable)
# %%
