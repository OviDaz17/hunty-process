# Proyecto de ETL con Google Cloud y Pandas

Este proyecto demuestra cómo extraer datos de archivos JSON, transformarlos en DataFrames de Pandas, y luego cargarlos en Google Cloud Storage y Google BigQuery.

## Supuestos y preferencias

# Esquema relacional
El esquema relacional desarrollado, contempla la totalidad de los datos siguiendo la siguiente cardinalidad:

-Tabla de Usuarios, uno a muchos.
-Tabla de Preguntas, uno a muchos.
-Tabla de Respuestas, muchos a uno.
-Tabla de Conversaciones, uno a muchos.

[Esquema relacional](https://drive.google.com/u/0/uc?id=1I0rK3ypEUzqjtaq-RSap8rHIUb-aOZ7L&export=download)

# Formato de datos Parquet
Se utilizó el formato de datos Parquet ya que es altamente eficiente, ofrece ventajas en compresión, rendimiento en lectura, compatibilidad con el werehouse utilizado.

# Inicio de seccion en google cloud
El codigo consumió las credenciales através de un archivo json (metodo                    from_service_account_json, se requiere archivo json con las credenciales, dicho archivo debe    estar ubicado en el directorio del codigo fuente)

# Adecuación google cloud
El proyecto, el bucket y las respectivas tablas fueron creadas desde la consola web de google cloud.

## Requisitos

- Python 3.6 o superior
- librerias disponibles en el arhivo requirements.txt (pip install -r requirements.txt)
- Credenciales de Google Cloud con acceso a BigQuery y Cloud Storage

## Instalación

1. Clona este repositorio.
2. Crea un archivo `.env` en la raíz del proyecto con las siguientes variables:
   - `credentials_JSON_Path`: Ruta al archivo de credenciales de Google Cloud.
   - `bucket_name`: Nombre del bucket de Google Cloud Storage.
   - `users_table_id`, `questions_table_id`, `answers_table_id`, `conversations_table_id`: IDs de las tablas en BigQuery.
   - `data_set`: ID del conjunto de datos en BigQuery.

## Uso

1. Ejecuta el script principal con `python main.py`.
2. El script cargará los datos de `anonymized_collector_selector_data.json` y `anonymized_raw_conversations_data.json`, transformará los datos y los cargará en Google Cloud Storage y BigQuery.

## Estructura del Proyecto

- `main.py`: Script principal que realiza la extracción, transformación y carga de los datos.
- `anonymized_collector_selector_data.json`: Archivo de entrada con datos de usuarios y preguntas.
- `anonymized_raw_conversations_data.json`: Archivo de entrada con datos de conversaciones.

## Visualizacion

Unas de las posibles visualizaciónes de los resultados se realizaron en Looker Studio y pueden observarse en este link  [Looker](https://lookerstudio.google.com/reporting/2ba8ad9f-8170-472f-b1a2-9829eff6d963)

## Notas adicionales

- Asegúrate de tener los permisos adecuados en Google Cloud para acceder a BigQuery y Cloud Storage.
- Este proyecto utiliza PyArrow para la serialización de DataFrames a Parquet, lo cual es necesario para la carga en BigQuery.