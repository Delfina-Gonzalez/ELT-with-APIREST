from pathlib import Path
import pandas as pd
from src.config import get_partition_path

def save_to_parquet(df_new: pd.DataFrame, endpoint_name: str, layer="bronze", incremental=True, mode="append", key_col=None):
    """
    Guarda un DataFrame en formato Parquet en el data lake.

    Args:
        df_new (pd.DataFrame): DataFrame con los datos nuevos a guardar.
        endpoint_name (str): Nombre del endpoint de la API.
        layer (str): Capa del data lake ("bronze", "silver", "gold").
        incremental (bool): Si True, busca histórico y solo agrega lo nuevo.
        mode (str): "overwrite" = reemplaza, "append" = agrega lo nuevo.
        key_col (str | list, opcional): Columna(s) clave para detectar duplicados.
    """
    if df_new.empty:
        print(f"[WARN] DataFrame vacío para {endpoint_name}, no se guarda.")
        return

    # Crear carpeta destino
    path = get_partition_path(layer, endpoint_name, incremental=False) 
    Path(path).mkdir(parents=True, exist_ok=True)

    file = Path(path) / f"{endpoint_name}.parquet"

    # FULL → sobrescribe todo
    if mode == "overwrite" or not file.exists():
        df_new.to_parquet(file, index=False)
        print(f"[INFO] Guardado FULL en {file}")
        return

    # INCREMENTAL → leer histórico y concatenar lo nuevo
    if mode == "append":
        try:
            df_old = pd.read_parquet(file)
        except Exception:
            df_old = pd.DataFrame()

        if not df_old.empty:
            # Unificar
            df_all = pd.concat([df_old, df_new], ignore_index=True)

            # Eliminar duplicados en base a key_col (si existe)
            if key_col:
                df_all = df_all.drop_duplicates(subset=key_col, keep="last")
            else:
                df_all = df_all.drop_duplicates()

            df_all.to_parquet(file, index=False)
            print(f"[INFO] Guardado INCREMENTAL en {file} ({len(df_new)} nuevos, total {len(df_all)})")
        else:
            df_new.to_parquet(file, index=False)
            print(f"[INFO] Guardado inicial en {file} ({len(df_new)} registros)")



