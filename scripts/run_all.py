# scripts/run_all.py
import subprocess

subprocess.run(["python", "scripts/01_bronze_layer.py"])
subprocess.run(["python", "scripts/02_silver_layer.py"])
subprocess.run(["python", "scripts/03_gold_layer.py"])