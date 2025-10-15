# Databricks notebook source
# MAGIC %md
# MAGIC ## Importación de funciones necesarias

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, trim, upper, initcap, to_date, when, split, lpad, length, concat_ws, datediff, lit, current_timestamp  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importación de los dataframe "df_compras" y "df_detalles" proveniente de las tablas "linio.silver_compras" y "linio.silver_detalles"

# COMMAND ----------

df_compras = spark.table("linio.silver_compras")
df_detalles = spark.table("linio.silver_detalles")

# Combinar df_compras y df_detalles por columna factura (inner join)
df_fact_compras = df_compras.join(df_detalles, on = "factura", how = "inner")

display(df_fact_compras.limit(10))

# COMMAND ----------

print(f"Cantidad registros df_compras: {df_compras.count()}")
print(f"Cantidad registros df_detalles: {df_detalles.count()}")
print(f"Cantidad registros df_fact_compras (join inner): {df_fact_compras.count()}")

# COMMAND ----------

df_fact_compras = df_fact_compras.select(
                "venta_id", "factura", "fecha_orden", "fecha_envio", "estado", "metodo_pago", "grupo_dias_abierto",
                "cliente_id", "tipo_documento", "num_documento", "nombre_cliente", "tipo_cliente", "vendedor",
                "departamento", "detalle_id", "categoria", "subcategoria", "producto", "unidades", "subtotal"
) #revisar columna tienda

df_fact_compras = df_fact_compras.withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

df_fact_compras.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación y poblado de las tablas delta en el esquema linio

# COMMAND ----------

try:
    # Asegurarse de que el esquema linio exista
    spark.sql("CREATE SCHEMA IF NOT EXISTS linio")

    
    # 1. Cargar gold_fact_compras
    spark.sql("DROP TABLE IF EXISTS linio.gold_fact_compras")

    df_compras.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .saveAsTable("linio.gold_fact_compras")

    print("✅ Tabla linio.gold_fact_compras creada y poblada.")

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

    MAX_LEN = 500
    if len(error_msg_full) > MAX_LEN:
        error_msg = f"{error_msg_full[:MAX_LEN]}\n[...] ERROR TRUNCADO ({len(error_msg_full)} caracteres) [...]"
    else:
        error_msg = error_msg_full

    dbutils.jobs.taskValues.set(key="error", value=error_msg)
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC