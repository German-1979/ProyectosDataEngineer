import subprocess

scripts = [
    "scripts/01_bronze_layer.py",
    "scripts/02_silver_layer.py",
    "scripts/03_gold_layer.py"
]

for script in scripts:
    print(f"\n🚀 Ejecutando {script} ...")
    try:
        subprocess.run(["python3", script], check=True)
        print(f"✅ Finalizado correctamente: {script}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error en {script}. Deteniendo ejecución.")
        break