# Base Python 3.11 slim
FROM python:3.11-slim

# Instalar Java 21 (necesario para Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk wget curl git && \
    rm -rf /var/lib/apt/lists/*

# Variables de entorno Java y Python
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Crear directorio de la app
WORKDIR /app

# Copiar requirements e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . /app

# Comando por defecto → ejecutar todo el pipeline
CMD ["python3", "scripts/run_all.py"]