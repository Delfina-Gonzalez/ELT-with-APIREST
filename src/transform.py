import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Asegúrate de importar DATA_RAW de tu módulo de configuración
from src.config import DATA_RAW

def load_from_delta(endpoint_name: str) -> pd.DataFrame:
    """
    Lee un dataset de formato Delta Lake desde el data lake y lo devuelve como un DataFrame de Pandas.

    Args:
        endpoint_name (str): El nombre del endpoint de la API, que corresponde al nombre
                             del subdirectorio en la capa 'raw' del data lake.

    Returns:
        pd.DataFrame: Un DataFrame de Pandas con los datos cargados.
    """
    # Inicialización de Spark con Delta
    builder = SparkSession.builder \
        .appName(f"Carga desde Delta {endpoint_name}") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Lee el directorio Delta Lake
    df_spark = spark.read.format("delta").load(str(DATA_RAW / endpoint_name))

    # Convierte el DataFrame de Spark a Pandas
    df_pandas = df_spark.toPandas()

    spark.stop()
    return df_pandas

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas duplicadas de un DataFrame de Pandas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.

    Returns:
        pd.DataFrame: Un nuevo DataFrame sin filas duplicadas.
    """
    return df.drop_duplicates()

def handle_nulls(df: pd.DataFrame, cols: list, fill_value=None) -> pd.DataFrame:
    """
    Rellena los valores nulos en columnas específicas de un DataFrame.

    Esta función toma un DataFrame, una lista de nombres de columnas y un valor
    para reemplazar los valores nulos en esas columnas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        cols (list): Una lista de los nombres de las columnas a procesar.
        fill_value (any, opcional): El valor con el que se llenarán los nulos.
            Por defecto es None.

    Returns:
        pd.DataFrame: El DataFrame con los valores nulos rellenados.
    """
    return df.fillna({col: fill_value for col in cols})

def rename_columns(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    """
    Renombra columnas de un DataFrame de Pandas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        rename_map (dict): Un diccionario donde las claves son los nombres
            actuales de las columnas y los valores son los nuevos nombres.

    Returns:
        pd.DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    return df.rename(columns=rename_map)

def create_new_column(df: pd.DataFrame, new_col: str, func) -> pd.DataFrame:
    """
    Crea una nueva columna en un DataFrame aplicando una función a cada fila.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        new_col (str): El nombre de la nueva columna a crear.
        func (callable): Una función que se aplicará a cada fila del DataFrame.

    Returns:
        pd.DataFrame: El DataFrame con la nueva columna agregada.
    """
    df[new_col] = df.apply(func, axis=1)
    return df

def join_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key: str, how="left") -> pd.DataFrame:
    """
    Combina dos DataFrames de Pandas basados en una columna clave.

    Utiliza el método `merge` para unir dos DataFrames de manera similar a un
    JOIN de SQL.

    Args:
        df1 (pd.DataFrame): El DataFrame izquierdo.
        df2 (pd.DataFrame): El DataFrame derecho.
        key (str): El nombre de la columna clave en la que se realizará el JOIN.
        how (str, opcional): El tipo de join a realizar ('left', 'right', 'outer', 'inner').
            Por defecto es 'left'.

    Returns:
        pd.DataFrame: El DataFrame resultante de la unión.
    """
    return df1.merge(df2, on=key, how=how)
