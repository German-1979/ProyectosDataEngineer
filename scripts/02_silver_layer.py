from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap, to_date, when, split, lpad, length, concat_ws,
    datediff, lit, current_timestamp
)
import traceback

# ==============================
# CONFIGURACIÓN INICIAL
# ==============================
spark = SparkSession.builder \
    .appName("Silver Layer") \
    .getOrCreate()

# Parámetro de fecha de carga
fecha_carga_str = "2025-06-16"  # puedes parametrizarlo luego con sys.argv o variable de entorno

try:
    fecha_carga = datetime.strptime(fecha_carga_str, "%Y-%m-%d").date()
    print(f"✅ Fecha de carga válida: {fecha_carga}")
except ValueError:
    raise ValueError("❌ El formato de fecha_carga debe ser YYYY-MM-DD")

# ==============================
# LECTURA DE DATOS DESDE BRONZE
# ==============================
try:
    columnas_requeridas = [
        "venta_id", "factura", "fecha_orden", "fecha_entrega", "fecha_envio",
        "estado", "cliente_code", "tipo_cliente", "nombres", "apellidos",
        "vendedor", "departamento", "metodo_pago", "tipo_compra"
    ]

    df_compras = (
        spark.read.parquet("data/bronze/linio_bronze_compras")
        .select([col(c) for c in columnas_requeridas])
    )

    print("✅ Datos cargados desde Bronze (compras)")
except Exception as e:
    print("❌ Error al leer Bronze compras:", e)
    spark.stop()
    raise e

# ==============================
# LIMPIEZA Y TRANSFORMACIONES
# ==============================
def parse_date_conditional(c):
    return when(
        c.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(c, "dd/MM/yyyy")
    ).when(
        c.rlike(r"^\d{2}-\d{2}-\d{2}$"), to_date(c, "dd-MM-yy")
    ).otherwise(None)

df_compras = (
    df_compras
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

# Estados
df_compras = df_compras.withColumn(
    "estado",
    when(col("estado") == 1, "Creado")
    .when(col("estado") == 2, "En Curso")
    .when(col("estado") == 3, "Programado")
    .when(col("estado") == 4, "Cancelado")
    .when(col("estado") == 5, "Entregado")
    .otherwise("No Definido")
)

# Cliente
df_compras = (
    df_compras
    .withColumn("cliente_id", split(col("cliente_code"), "-").getItem(0).cast("int"))
    .withColumn("num_documento", trim(split(col("cliente_code"), "-").getItem(1)))
)

df_compras = df_compras.withColumn(
    "num_documento",
    when(length("num_documento") < 8, lpad("num_documento", 8, "0"))
    .otherwise(col("num_documento"))
)

df_compras = df_compras.withColumn(
    "tipo_documento",
    when(length("num_documento") == 8, "DNI")
    .when((length("num_documento") == 11) & col("num_documento").startswith("10"), "RUC10")
    .when((length("num_documento") == 11) & col("num_documento").startswith("20"), "RUC20")
    .otherwise(None)
)

df_compras = df_compras.withColumn("nombre_cliente", concat_ws(" ", col("nombres"), col("apellidos")))

# Cálculo de días abiertos
estados_validos = ["Creado", "En Curso", "Programado"]

df_compras = df_compras.withColumn(
    "dias_abierto",
    when(col("estado").isin(estados_validos),
         datediff(lit(fecha_carga), col("fecha_orden"))
    ).otherwise(lit(None))
)

df_compras = df_compras.withColumn(
    "grupo_dias_abierto",
    when(col("dias_abierto").isNull(), None)
    .when(col("dias_abierto") <= 3, "[0 – 3 días]")
    .when((col("dias_abierto") >= 4) & (col("dias_abierto") <= 7), "[4 – 7 días]")
    .when(col("dias_abierto") >= 8, "[más de 8 días]")
    .otherwise(None)
)

df_compras = (
    df_compras.select(
        "venta_id", "factura", "tipo_compra", "fecha_orden", "fecha_entrega", "fecha_envio",
        "estado", "cliente_id", "tipo_documento", "num_documento", "nombre_cliente",
        "tipo_cliente", "vendedor", "departamento", "metodo_pago",
        "dias_abierto", "grupo_dias_abierto"
    )
    .withColumn("fecha_carga", current_timestamp())
)

print("✅ Transformaciones aplicadas sobre df_compras")

# ==============================
# DETALLES
# ==============================
try:
    df_detalles = spark.read.parquet("data/bronze/linio_bronze_detalles")

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
        .withColumn("subtotal", col("unidades") * col("precio_unitario"))
        .withColumn("fecha_carga", current_timestamp())
    )

    df_detalles = df_detalles.select(
        "detalle_id", "factura", "oferta_id", "categoria", "subcategoria",
        "producto", "unidades", "subtotal", "fecha_carga"
    )

    print("✅ Transformaciones aplicadas sobre df_detalles")
except Exception as e:
    print("❌ Error procesando detalles:", e)
    raise e

# ==============================
# ESCRITURA EN CAPA SILVER (Parquet)
# ==============================
try:
    df_compras.write.mode("overwrite").parquet("data/silver/linio_silver_compras")
    df_detalles.write.mode("overwrite").parquet("data/silver/linio_silver_detalles")
    print("✅ Archivos Parquet escritos en data/silver/")
except Exception as e:
    print("❌ Error al escribir en capa Silver:", e)
    raise e

finally:
    spark.stop()