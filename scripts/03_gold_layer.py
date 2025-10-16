from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Crear sesión Spark
spark = (
    SparkSession.builder
    .appName("Gold Layer")
    .config("spark.sql.session.timeZone", "America/Santiago")
    .getOrCreate()
)

# ===========================
# 1️⃣ Leer datos desde Silver
# ===========================
print("🔹 Leyendo datos desde capa Silver...")

df_compras = spark.read.parquet("/app/data/silver/linio_silver_compras")
df_detalles = spark.read.parquet("/app/data/silver/linio_silver_detalles")

print(f"Registros df_compras: {df_compras.count()}")
print(f"Registros df_detalles: {df_detalles.count()}")

# ===========================================
# 2️⃣ Combinar df_compras y df_detalles (JOIN)
# ===========================================
df_fact_compras = df_compras.join(df_detalles, on="factura", how="inner")

print(f"Registros después del JOIN: {df_fact_compras.count()}")

# ======================================
# 3️⃣ Seleccionar y preparar las columnas
# ======================================
df_fact_compras = df_fact_compras.select(
    "venta_id", "factura", "fecha_orden", "fecha_envio", "estado",
    "metodo_pago", "grupo_dias_abierto", "cliente_id", "tipo_documento",
    "num_documento", "nombre_cliente", "tipo_cliente", "vendedor",
    "departamento", "detalle_id", "categoria", "subcategoria",
    "producto", "unidades", "subtotal"
)

df_fact_compras = df_fact_compras.withColumn("fecha_carga", current_timestamp())

df_fact_compras.printSchema()

# ===========================================
# 4️⃣ Guardar resultado en carpeta GOLD (Parquet)
# ===========================================

try:
    df_fact_compras.write.mode("overwrite").parquet("/app/data/gold/fact_compras")

    print("✅ Archivo Parquet generado correctamente en fact_compras")

except Exception as e:
    print(f"❌ Error guardando archivo Parquet: {e}")

# ===========================================
# 5️⃣ Mostrar muestra de datos
# ===========================================
df_fact_compras.show(10, truncate=False)

spark.stop()