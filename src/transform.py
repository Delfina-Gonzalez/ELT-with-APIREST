import pandas as pd
import numpy as np
from pathlib import Path
from src.config import DATA_BRONZE, DATA_SILVER

def load_from_parquet(endpoint_name: str, layer="bronze") -> pd.DataFrame:
    """
    Lee un dataset en formato Parquet desde el data lake.
    """
    base = {"bronze": DATA_BRONZE, "silver": DATA_SILVER}[layer]
    path = Path(base) / endpoint_name
    files = list(path.rglob("*.parquet"))

    if not files:
        print(f"[WARN] No se encontraron archivos para {endpoint_name} en {layer}")
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

# Normalización de columnas complejas
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas con arrays numpy a strings.
    Esto permite aplicar drop_duplicates y otras transformaciones sin error.
    """

    for col in df:
        if df[col].apply(lambda x: isinstance(x, np.ndarray)).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, np.ndarray) else x)
    return df

# 🔹 Transformaciones
def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

import pandas as pd

def clean_dataframe(df: pd.DataFrame, null_threshold: float = 0.8) -> pd.DataFrame:
    """
    Limpieza completa de un DataFrame.
    
    Pasos:
    1. Elimina columnas completamente vacías.
    2. Elimina columnas irrelevantes (muchos nulos o un solo valor).
    3. Rellena nulos en columnas específicas.
    
    Args:
        df: DataFrame original.
        fill_cols: lista de columnas donde rellenar nulos.
        fill_value: valor para rellenar nulos en fill_cols.
        null_threshold: umbral de nulos para considerar una columna irrelevante.
        
    Returns:
        DataFrame limpio.
    """
    # 1️⃣ Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how='all')
    
    # 2️⃣ Eliminar columnas irrelevantes automáticamente
    high_null_cols = df.columns[df.isna().mean() > null_threshold].tolist()
    single_value_cols = df.columns[df.nunique() <= 1].tolist()
    cols_to_drop = list(set(high_null_cols + single_value_cols))
    df = df.drop(columns=cols_to_drop)
    
    # 3️⃣ Rellenar nulos en columnas específicas
    if "details" in df.columns:
        df = df.fillna({"details": "Sin descripción"})
    
    return df

def drop_columns(df: pd.DataFrame, df_type: str) -> pd.DataFrame:
    """
    Elimina columnas del DataFrame.
    """
    if df_type == "upcoming_launches":
        df.drop(columns=["fairings", "links", "crew",  "capsules", "payloads", "failures","crew","ships", "net", "window", "success", "static_fire_date_utc", "static_fire_date_unix"], errors='ignore', inplace=True)
    elif df_type == "rockets":
        df.drop(columns=["payload_weights","flickr_images"], errors='ignore', inplace=True)
    return df

def rename_columns(df: pd.DataFrame, endpoint_name: str) -> pd.DataFrame:
    
    rename_map = {}

    if endpoint_name == "rockets":
        rename_map = {
            "name": "Nombre",
            "type": "Tipo",
            "active": "Estado_Activo",
            "stages": "Etapa",
            "boosters": "Impulsores",
            "cost_per_launch": "Costo_por_Lanzamiento",
            "success_rate_pct": "Tasa_de_Suceso",
            "first_flight": "Fecha_del_Primer_Vuelo",
            "country": "País_de_Fabricación",
            "company": "Fabricante",
            "engines.number": "Numero_de_Motores",
            "engines.type": "Tipo_de_Motor",
            "engines.version": "Version_de_Motor",
            "engines.layout": "Disposición_de_Motor",
            "engines.engine_loss_max": "Pérdida_Máxima_de_Motor",
            "engines.propellant_1": "Propelente_Principal",
            "engines.propellant_2": "Propelente_Secundario",
            "engines.thrust_to_weight": "Relación_Impulso_Peso",
            "landing_legs.number": "Número_de_Patas_de_Aterrizaje",
            "landing_legs.material": "Material_de_Patas_de_Aterrizaje"
        }
    elif endpoint_name == "upcoming_launches":
        rename_map = {
            "name": "Nombre",
            "date_utc": "Fecha_Lanzamiento_UTC",
            "date_unix": "Fecha_Lanzamiento_Unix",
            "date_local": "Fecha_Lanzamiento_Local",
            "date_precision": "Precisión_de_Fecha",
            "upcoming": "Es_Proximo",
            "rocket": "ID_Cohete",
            "success": "Éxito",
            "details": "Detalles",
            "auto_update": "Auto_Actualización",
            "flight_number": "Número_de_Vuelo"
        }
    return df.rename(columns=rename_map)


