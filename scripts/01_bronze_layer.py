from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
from pyspark.sql.types import StringType
import re, traceback

# Crear SparkSession
spark = SparkSession.builder \
    .appName("Bronze Layer") \
    .getOrCreate()

try:
    # ===== COMPRAS PRESENCIAL =====
    compras_presencial = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "false")
        .load("data/raw/compras/Presencial.csv")
    )

    for c in compras_presencial.columns:
        nuevo_nombre = c.lower().replace(" ", "_")
        compras_presencial = compras_presencial.withColumnRenamed(c, nuevo_nombre)
    compras_presencial = compras_presencial.withColumnRenamed("ventaid", "venta_id")
    compras_presencial = compras_presencial.withColumn("tipo_compra", lit("Presencial"))
    compras_presencial = compras_presencial.withColumn("fecha_carga", current_timestamp())

    # ===== COMPRAS ONLINE =====
    compras_online = (
        spark.read.format("json")
        .option("multiline", "true")
        .load("data/raw/compras/Online.json")
    )

    for c in compras_online.columns:
        compras_online = compras_online.withColumn(c, col(c).cast(StringType()))

    def camel_to_snake(name):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    compras_online = compras_online.select([col(c).alias(camel_to_snake(c)) for c in compras_online.columns])
    compras_online = compras_online.withColumnRenamed("venta_i_d", "venta_id")
    compras_online = compras_online.withColumn("tipo_compra", lit("Online"))
    compras_online = compras_online.withColumn("fecha_carga", current_timestamp())

    # ===== UNION DE DATAFRAMES =====
    df_compras = compras_presencial.unionByName(compras_online, allowMissingColumns=True)

    # ===== DETALLES =====
    paths = [
        "data/raw/detalles/Alibaba.csv",
        "data/raw/detalles/Amazon.csv",
        "data/raw/detalles/GameStop.csv",
        "data/raw/detalles/Walmart.csv"
    ]

    df_detalles = spark.read.option("header", "true") \
        .option("delimiter", "|") \
        .option("inferSchema", "false") \
        .csv(paths)

    # Extraer nombre del archivo desde el path
    df_detalles = df_detalles.withColumn("nombre_archivo", expr("substring_index(input_file_name(), '/', -1)")) \
                             .withColumn("fecha_carga", current_timestamp())

    for c in df_detalles.columns:
        nuevo_nombre = c.lower().replace(" ", "_")
        df_detalles = df_detalles.withColumnRenamed(c, nuevo_nombre)


    # ===== CREAR BRONZE EN PARQUET =====
    df_compras.write.parquet("data/bronze/linio_bronze_compras", mode="overwrite")
    df_detalles.write.parquet("data/bronze/linio_bronze_detalles", mode="overwrite")

    print("✅ Bronze layer creada y poblada correctamente en Parquet.")

except Exception as e:
    print("❌ Error ejecutando Bronze layer:")
    print(traceback.format_exc())

finally:
    spark.stop()