# ETL Medallón con Spark

Este proyecto implementa un flujo ETL tipo medallón (Bronze → Silver → Gold) usando **PySpark**, **Docker** y **Python**.  
Está pensado para pruebas y aprendizaje de pipelines de datos automatizados.


## Estructura del proyecto

etl_medallon_spark/
│
├── data/                # Carpeta para datos (ignorados en Git)
│ ├── raw/               # Datos originales
│ ├── bronze/            # Datos procesados capa Bronze
│ ├── silver/            # Datos procesados capa Silver
│ └── gold/              # Datos finales (para análisis)
│
├── notebooks/           # Notebooks de prueba y desarrollo
├── src/                 # Código Python de ETL
│ └── run_all.py/        # Scripts por capa: 01_bronze, 02_silver, 03_gold
├── Dockerfile           # Imagen Docker para ejecución
├── docker-compose.yml   # Orquestación de contenedores
├── requirements.txt     # Dependencias Python
└── README.md            # Este archivo

---

## ⚙️ Requisitos

- **Python** >= 3.8  
- **Java JDK 8 o 11** (requerido por Spark)  
- **Docker** y **Docker Compose**  
- **PySpark**, **py4j**, entre otras dependencias listadas en `requirements.txt`

> Todos los recursos pueden correr en entorno local o VM en la nube para pruebas, considerando datos de tamaño moderado.

---

## 🏃‍♂️ Ejecución del proyecto

### 1. Construir la imagen Docker

docker-compose build

### 2. Ejecutar el flujo ETL completo

docker-compose up

Esto ejecutará automáticamente los scripts en el orden:
01_bronze_layer.py → 02_silver_layer.py → 03_gold_layer.py

### 3. Automatización con run_all.py

python src/run_all.py

Esto garantiza que los scripts corran en secuencia, registrando logs y errores.


📝 Buenas prácticas incluidas

Arquitectura medallón: separación clara de capas de datos
Dockerización: reproducibilidad y portabilidad
Estandarización de scripts: ejecución secuencial y modular
Control de versiones Git: ignorando datos pesados y temporales
Documentación clara: README profesional para compartir y colaborar


👤 Autor

Germán – Ingeniero Civil Industrial, especialista en análisis de datos, procesos ETL y automatización de pipelines.

📖 Referencias y recursos

PySpark Documentation
Docker Documentation
Docker Compose Documentation