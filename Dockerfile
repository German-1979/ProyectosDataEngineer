FROM python:3.11-slim

# Instalamos Java necesario para Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-11-jdk-headless \
       wget \
       curl \
       ca-certificates \
       gnupg \
       lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Definimos variables de entorno para Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Instalamos PySpark y dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el proyecto
WORKDIR /app
COPY . /app

# Comando por defecto para ejecutar
CMD ["python", "scripts/run_all.py"]