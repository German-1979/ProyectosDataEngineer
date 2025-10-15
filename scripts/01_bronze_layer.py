# Databricks notebook source
# MAGIC %md
# MAGIC ## **Importación de funciones necesarias**

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ##  **Importación de archivo "Presencial.csv**

# COMMAND ----------

compras_presencial = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "false")  
    .load("/Volumes/workspace/linio/compras/Presencial.csv")
)

# COMMAND ----------

# Renombrar columnas: minúsculas y reemplazo de espacios por "_"
for c in compras_presencial.columns:
    nuevo_nombre = c.lower().replace(" ", "_")
    compras_presencial = (compras_presencial.withColumnRenamed(c, nuevo_nombre)
                                            .withColumnRenamed("ventaid", "venta_id")
    )

 
 # Agreguemps las columnas de trazabilidad
compras_presencial = (compras_presencial 
    .withColumn("tipo_compra", lit("Presencial")) 
    .withColumn("fecha_carga", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Importación de archivo "Online.json**

# COMMAND ----------

compras_online = (spark.read.format("json") 
    .option("multiline", "true") 
    .load("/Volumes/workspace/linio/compras/Online.json")
)

# Convertir todas las columnas a StringType
for c in compras_online.columns:
    compras_online = compras_online.withColumn(c, col(c).cast(StringType()))

# COMMAND ----------

# Renombrar columnas: minúsculas y separación de plabras por "_" mediante función camel_to_snake    
def camel_to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

compras_online = (compras_online
                  .select([col(c).alias(camel_to_snake(c)) for c in compras_online.columns])
)

# Arreglando el nomre de columna "venta_i_d" por "venta_id"
compras_online = compras_online.withColumnRenamed("venta_i_d", "venta_id")

# Agreguemps las columnas de trazabilidad
compras_online = (compras_online 
    .withColumn("tipo_compra", lit("Online")) 
    .withColumn("fecha_carga", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  **Union de los dataframes compras_presencial y compras_online**

# COMMAND ----------

df_compras = compras_presencial.unionByName(compras_online, allowMissingColumns=True)
display(df_compras.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Importación masiva de los archivos "CSV" ubicados en el volumen "detalles"**

# COMMAND ----------

paths = [
    "/Volumes/workspace/linio/detalles/Alibaba.csv", "/Volumes/workspace/linio/detalles/Amazon.csv", 
    "/Volumes/workspace/linio/detalles/GameStop.csv", "/Volumes/workspace/linio/detalles/Walmart.csv"
    ]

for path in paths:
    spark.read.option("header", "true").csv(path).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC  Todos los archivos tienen la misma cantidad de columnas y además están en el mismo orden

# COMMAND ----------

# Leer todos los CSV con columnas como StringType
df_detalles = spark.read.option("header", "true") \
               .option("delimiter", "|") \
               .option("inferSchema", "false") \
               .csv(paths)

# Usar _metadata.file_path en lugar de input_file_name
df_detalles = df_detalles.withColumn("nombre_archivo",
                   expr("substring_index(_metadata.file_path, '/', -1)")) \
       .withColumn("fecha_carga", current_timestamp())

# Renombrar columnas: minúsculas y reemplazo de espacios por "_"
for c in df_detalles.columns:
    nuevo_nombre = c.lower().replace(" ", "_")
    df_detalles = df_detalles.withColumnRenamed(c, nuevo_nombre)

display(df_detalles.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación de Bronze_Layer asociado a "compras" y "detalles"

# COMMAND ----------

try:
    # Asegurarse de que el esquema linio exista
    spark.sql("CREATE SCHEMA IF NOT EXISTS linio")

    
    # 1. Cargar bronze_compras
    spark.sql("DROP TABLE IF EXISTS linio.bronze_compras")

    df_compras.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .saveAsTable("linio.bronze_compras")

    print("✅ Tabla linio.bronze_compras creada y poblada.")

    
    # 2. Cargar bronze_detalles
    spark.sql("DROP TABLE IF EXISTS linio.bronze_detalles")

    df_detalles.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .saveAsTable("linio.bronze_detalles")

    print("✅ Tabla linio.bronze_detalles creada y poblada.")


    dbutils.jobs.taskValues.set(key="estado", value="OK")

except Exception as e:
    import traceback
    # Tipo de error
    error_type = type(e).__name__
    # Descripcion del error
    error_summary = str(e)
    # Traza del error (ver en que parte se generó el error)
    error_trace = traceback.format_exc()
    
    # Error completo
    error_msg_full = f"{error_type}: {error_summary}\n{error_trace}"

    if len(error_msg_full) > 500:
        error_msg = error_msg_full[:500] + "\n[...] ERROR TRUNCADO [...]"
    else:
        error_msg = error_msg_full

    dbutils.jobs.taskValues.set(key="error", value=error_msg)
    raise e