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

def handle_nulls(df: pd.DataFrame, cols: list, fill_value=None) -> pd.DataFrame:
    return df.fillna({col: fill_value for col in cols})

def rename_columns(df: pd.DataFrame, endpoint_name: str) -> pd.DataFrame:
    
    rename_map = {}

    if endpoint_name == "rockets":
        rename_map = {
            "name": "rocket_name",
            "type": "rocket_type",
            "active": "is_active",
            "stages": "num_stages",
            "boosters": "num_boosters",
            "cost_per_launch": "launch_cost_usd",
            "success_rate_pct": "success_rate_percent",
            "first_flight": "first_flight_date",
            "country": "manufacturing_country",
            "company": "manufacturer",
            "payload_weights": "payload_weights_info",
            "flickr_images": "image_urls",
            "engines.number": "engines_count",
            "engines.type": "engine_type",
            "engines.version": "engine_version",
            "engines.layout": "engine_layout",
            "engines.engine_loss_max": "engine_loss_max",
            "engines.propellant_1": "propellant_primary",
            "engines.propellant_2": "propellant_secondary",
            "engines.thrust_to_weight": "thrust_to_weight_ratio",
            "landing_legs.number": "landing_legs_count",
            "landing_legs.material": "landing_legs_material"
        }
    return df.rename(columns=rename_map)

def expand_payload_weights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expande la columna 'payload_weights' en columnas separadas,
    dejando solo valores en kilogramos (kg).
    Ejemplo: payload_leo_kg, payload_gto_kg, payload_mars_kg...
    """
    if "payload_weights_info" not in df.columns:
        return df

    # Crear columnas nuevas solo en kg
    for idx, row in df.iterrows():
        if isinstance(row["payload_weights_info"], list):
            for payload in row["payload_weights_info"]:
                name = payload.get("id", payload.get("name", "unknown")).lower()
                df.at[idx, f"payload_{name}_kg"] = payload.get("kg")

    return df

