from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path
from src.config import get_partition_path

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("Proyecto ELT SpaceX") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def save_to_delta(df: pd.DataFrame, endpoint_name: str, incremental: bool = True, mode: str = "append"):
    """
    Guarda un DataFrame de Pandas en formato Delta Lake.

    La función convierte un DataFrame de Pandas en un Spark DataFrame y lo guarda
    en un directorio específico, ya sea de forma incremental (con partición por fecha)
    o completa, usando el formato Delta Lake.

    Args:
        df (pd.DataFrame): El DataFrame de Pandas que se va a guardar.
        endpoint_name (str): El nombre del endpoint de la API, usado para determinar la ruta de guardado.
        incremental (bool, opcional): Si es True, guarda los datos en una ruta particionada
            por año, mes y día. Si es False, guarda en una ruta fija. Por defecto es True.
        mode (str, opcional): El modo de escritura para Spark. Puede ser 'append'
            (añade los datos a la tabla existente) o 'overwrite' (reemplaza los datos
            existentes). Por defecto es 'append'.
    
    Returns:
        None
    """
    path = get_partition_path(endpoint_name, incremental)
    Path(path).mkdir(parents=True, exist_ok=True)
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode(mode).save(path)