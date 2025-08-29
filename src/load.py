from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd
import json
from src.config import get_partition_path

def save_to_delta(df: pd.DataFrame, endpoint_name: str, incremental: bool = True, mode="overwrite"):
    """
    Configura sesión de Spark
    Guarda un DataFrame de Pandas en formato Delta Lake.
    Soporta partición por fecha si incremental=True.
    """
    # Detener cualquier Spark previo
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()
    except:
        pass

    # Inicialización de Spark con Delta
    builder = SparkSession.builder \
        .appName("Proyecto ELT SpaceX") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # 1. No guardar si está vacío
    if df.empty:
        print(f"[WARN] DataFrame vacío para {endpoint_name}, no se guarda nada.")
        return

    # 2. Normalización columnas para Spark
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    # 3. Crear carpeta destino usando partición
    path = get_partition_path(endpoint_name, incremental)
    Path(path).mkdir(parents=True, exist_ok=True)

    # 4. Pandas -> Spark
    spark_df = spark.createDataFrame(df)

    # 5. Guardar en Delta
    spark_df.write.format("delta").mode(mode).save(path)
    print(f"[INFO] Guardado correcto: {endpoint_name} en {path}")

