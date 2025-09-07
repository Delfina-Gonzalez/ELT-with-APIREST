from pathlib import Path
import pandas as pd
from src.config import get_partition_path
import numpy as np
import json

def make_hashable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte listas, diccionarios y arrays de NumPy en strings JSON
    para que sean hashables (necesario para drop_duplicates y Parquet).
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict, np.ndarray))).any():
            df[col] = df[col].apply(
                lambda x: (
                    json.dumps(x.tolist()) if isinstance(x, np.ndarray)
                    else json.dumps(x) if isinstance(x, (list, dict))
                    else x
                )
            )
    return df


def save_to_parquet(
    df_new: pd.DataFrame,
    endpoint_name: str,
    incremental: bool = False,
    layer="bronze",
    mode="append",
    key_col=None
):
    """
    Guarda un DataFrame en formato Parquet en el data lake,
    gestionando cargas FULL e INCREMENTALES.
    """

    if df_new.empty:
        print(f"[WARN] DataFrame vacío para {endpoint_name}, no se guarda.")
        return

    # Normalizar datos hashables
    df_new = make_hashable(df_new)

    # Obtener path y flag update
    path, update = get_partition_path(layer, endpoint_name, incremental)

    if update:
        print(f"[INFO] Salteando guardado para {endpoint_name}, no hay actualización.")
        return  # 👈 corta acá

    # Crear carpeta destino
    Path(path).mkdir(parents=True, exist_ok=True)
    file_path = Path(path) / f"{endpoint_name}.parquet"

    if not incremental:
        # --- CARGA FULL ---
        if mode == "overwrite" or not file_path.exists():
            df_new.to_parquet(file_path, index=False)
            print(f"[INFO] Guardado FULL (overwrite) en {file_path}. Registros: {len(df_new)}")
        elif mode == "append":
            try:
                df_old = pd.read_parquet(file_path)
                df_old = make_hashable(df_old)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
                df_all = df_all.drop_duplicates(subset=key_col, keep="last") if key_col else df_all.drop_duplicates()
                df_all.to_parquet(file_path, index=False)
                print(f"[INFO] Guardado FULL (append) en {file_path}. Total: {len(df_all)}")
            except FileNotFoundError:
                df_new.to_parquet(file_path, index=False)
                print(f"[INFO] Guardado inicial FULL en {file_path}. Registros: {len(df_new)}")
        return

    # --- CARGA INCREMENTAL ---
    try:
        df_old = pd.read_parquet(file_path)
        df_old = make_hashable(df_old)
    except FileNotFoundError:
        df_old = pd.DataFrame()
        print(f"[INFO] Primera carga incremental para {endpoint_name}.")
    except Exception as e:
        print(f"[ERROR] Problema leyendo {file_path}: {e}")
        df_old = pd.DataFrame()

    if not df_old.empty:
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        if key_col:
            if isinstance(key_col, str):
                key_col = [key_col]
            df_all = df_all.drop_duplicates(subset=key_col, keep="last")
            print(f"[INFO] Duplicados eliminados usando {key_col}.")
        else:
            df_all = df_all.drop_duplicates(keep="last")
            print(f"[INFO] Duplicados eliminados (todas las columnas).")
        df_all.to_parquet(file_path, index=False)
        print(f"[INFO] Guardado INCREMENTAL en {file_path}. Nuevos: {len(df_new)}, Total: {len(df_all)}")
    else:
        df_new.to_parquet(file_path, index=False)
        print(f"[INFO] Guardado inicial INCREMENTAL en {file_path}. Registros: {len(df_new)}")
