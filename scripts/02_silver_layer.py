# Databricks notebook source
# MAGIC %md
# MAGIC ## **Importación de funciones necesarias**

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, trim, upper, initcap, to_date, when, split, lpad, length, concat_ws, datediff, lit, current_timestamp       

# COMMAND ----------

# Obtener el parámetro de entrada 'fecha_carga'
dbutils.widgets.text("fecha_carga", "2025-06-16", "Fecha de carga")
fecha_carga_str = dbutils.widgets.get("fecha_carga")

# Convertir a tipo date
try:
    fecha_carga = datetime.strptime(fecha_carga_str, "%Y-%m-%d").date()
    print(f"✅ Fecha de carga válida: {fecha_carga}")
except ValueError:
    raise ValueError("❌ El formato de fecha_carga debe ser YYYY-MM-DD")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trabajo sobre el dataframe "df_compras" proveniente de 01_Bronze_Layer

# COMMAND ----------

# Leer columnas específicas desde la tabla Delta
columnas_requeridas = [
    "venta_id", "factura", "fecha_orden", "fecha_entrega", "fecha_envio",
    "estado", "cliente_code", "tipo_cliente", "nombres", "apellidos",
    "vendedor", "departamento", "metodo_pago", "tipo_compra"
]

df_compras = (
    spark.read
    .table("linio.bronze_compras")
    .select([col(c) for c in columnas_requeridas])
)

# COMMAND ----------

# Dando formato a las columnas
def parse_date_conditional(c):
    return when(
        c.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(c, "dd/MM/yyyy")
    ).when(
        c.rlike(r"^\d{2}-\d{2}-\d{2}$"), to_date(c, "dd-MM-yy")
    ).otherwise(None)

df_compras = (df_compras 
    .withColumn("venta_id", col("venta_id").cast("int")) 
    .withColumn("estado", col("estado").cast("int")) 

    .withColumn("fecha_orden", to_date(col("fecha_orden"), "yyyy-MM-dd")) 
    .withColumn("fecha_entrega", parse_date_conditional(trim(col("fecha_entrega")))) 
    .withColumn("fecha_envio", parse_date_conditional(trim(col("fecha_envio")))) 

    .withColumn("factura", upper(trim(col("factura")))) 
    .withColumn("tipo_cliente", upper(trim(col("tipo_cliente")))) 
    .withColumn("nombres", initcap(trim(col("nombres")))) 
    .withColumn("apellidos", initcap(trim(col("apellidos")))) 
    .withColumn("vendedor", trim(col("vendedor"))) 
    .withColumn("departamento", trim(col("departamento"))) 
    .withColumn("metodo_pago", trim(col("metodo_pago")))
)

# COMMAND ----------

df_compras.printSchema()


# COMMAND ----------

# Apliquemos las transformaciones solicitadas
df_compras = (df_compras.withColumn(
    "estado",
     when(col("estado") == 1, "Creado")
    .when(col("estado") == 2, "En Curso")
    .when(col("estado") == 3, "Programado")
    .when(col("estado") == 4, "Cancelado")
    .when(col("estado") == 5, "Entregado")
    .otherwise("No Definido"))
)

df_compras = (df_compras 
    .withColumn("cliente_id", split(col("cliente_code"), "-").getItem(0).cast("int"))
    .withColumn("num_documento", trim(split(col("cliente_code"), "-").getItem(1)))
)

df_compras = (df_compras.withColumn(
    "num_documento",
    when(length("num_documento") < 8, lpad("num_documento", 8, "0"))
    .otherwise(col("num_documento")))
)

df_compras = (df_compras.withColumn(
    "tipo_documento",
    when(length("num_documento") == 8, "DNI")
    .when((length("num_documento") == 11) & col("num_documento").startswith("10"), "RUC10")
    .when((length("num_documento") == 11) & col("num_documento").startswith("20"), "RUC20")
    .otherwise(None))
)

df_compras = (df_compras.withColumn(
    "nombre_cliente",
    concat_ws(" ", col("nombres"), col("apellidos")))
)

estados_validos = ["Creado", "En Curso", "Programado"]

df_compras = (df_compras.withColumn(
    "dias_abierto",
    when(col("estado").isin(estados_validos),
         datediff(lit(fecha_carga), col("fecha_orden"))
    ).otherwise(lit(None)))
)

df_compras = (df_compras.withColumn(
    "grupo_dias_abierto",
    when(col("dias_abierto").isNull(), None)
    .when(col("dias_abierto") <= 3, "[0 – 3 días]")
    .when((col("dias_abierto") >= 4) & (col("dias_abierto") <= 7), "[4 – 7 días]")
    .when(col("dias_abierto") >= 8, "[más de 8 días]")
    .otherwise(None))
)

columnas_requeridas = [
    "venta_id", "factura", "tipo_compra", "fecha_orden", "fecha_entrega", "fecha_envio",
    "estado", "cliente_id", "tipo_documento", "num_documento", "nombre_cliente",
    "tipo_cliente", "vendedor", "departamento", "metodo_pago","dias_abierto", "grupo_dias_abierto"
]

df_compras = (df_compras.select(columnas_requeridas)
              .withColumn("fecha_carga", current_timestamp())
)

display(df_compras.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trabajo sobre el dataframe "df_detalles" proveniente de 01_Bronze_Layer

# COMMAND ----------

df_detalles = (
    spark.read
    .table("linio.bronze_detalles")
)  

# COMMAND ----------

# Dando formato a las columnas
df_detalles = (
    df_detalles
    .withColumn("detalle_id", col("detalle_id").cast("int"))
    .withColumn("unidades", col("unidades").cast("int"))
    .withColumn("oferta_id", col("oferta_id").cast("int"))
    .withColumn("precio_unitario", col("precio_unitario").cast("double"))
    .withColumn("factura", upper(trim(col("factura"))))
    .withColumn("categoria", trim(col("categoria")))
    .withColumn("subcategoria", trim(col("subcategoria")))
    .withColumn("producto", trim(col("producto")))
)

# COMMAND ----------

df_detalles.printSchema()

# COMMAND ----------

# Apliquemos las transformaciones solicitadas
df_detalles = df_detalles.withColumn("subtotal", col("unidades") * col("precio_unitario"))
df_detalles = df_detalles.withColumn("tienda", split(col("nombre_archivo"), "\.").getItem(0))

# Seleccionar columnas finales (factura aparece dos veces, lo corregí a una sola)
df_detalles = df_detalles.select(
    "detalle_id", "factura", "oferta_id", "categoria", "subcategoria",
    "producto", "unidades", "subtotal"
)

df_detalles = df_detalles.withColumn("fecha_carga", current_timestamp())

display(df_detalles.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación y poblado de las tablas delta en el esquema linio

# COMMAND ----------

try:
    # Asegurarse de que el esquema linio exista
    spark.sql("CREATE SCHEMA IF NOT EXISTS linio")

    
    # 1. Cargar silver_compras
    spark.sql("DROP TABLE IF EXISTS linio.silver_compras")

    df_compras.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("linio.silver_compras")

    print("✅ Tabla linio.silver_compras creada y poblada.")

    
    # 2. Cargar silver_detalles
    spark.sql("DROP TABLE IF EXISTS linio.silver_detalles")

    df_detalles.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("linio.silver_detalles")

    print("✅ Tabla linio.silver_detalles creada y poblada.")

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