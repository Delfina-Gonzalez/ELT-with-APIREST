from pathlib import Path
import pandas as pd
from src.config import get_partition_path

def save_to_parquet(df_new: pd.DataFrame, endpoint_name: str, incremental: bool = False, layer="bronze", mode="append", key_col=None):
    """
    Guarda un DataFrame en formato Parquet en el data lake, gestionando
    sobrescritura y agregación de historiales.

    Args:
        df_new (pd.DataFrame): DataFrame con los datos nuevos a guardar.
        endpoint_name (str): Nombre del endpoint de la API.
        layer (str): Capa del data lake ("bronze", "silver", "gold").
        incremental (bool): Si True, busca histórico y solo agrega lo nuevo.
                            Si False, siempre se considera un "full" load.
        mode (str): "overwrite" = reemplaza todo el contenido existente.
                    "append" = agrega lo nuevo al historial existente.
        key_col (str | list, opcional): Columna(s) clave para detectar duplicados
                                        en cargas incrementales.
    """
    if df_new.empty:
        print(f"[WARN] DataFrame vacío para {endpoint_name}, no se guarda.")
        return

    # Determinar la ruta base del endpoint
    # Si incremental es False, siempre guardaremos en la carpeta "full"
    path = get_partition_path(layer, endpoint_name, incremental)
    Path(path).mkdir(parents=True, exist_ok=True)

    # Construir la ruta del archivo principal
    file_path = Path(path) / f"{endpoint_name}.parquet"

    if not incremental:
        # Carga FULL: sobrescribe el archivo existente
        df_new.to_parquet(file_path, index=False)
        print(f"[INFO] Guardado FULL ({mode}) en {file_path}. Registros: {len(df_new)}")
        return

    # Carga INCREMENTAL (is_full_load es False y mode es "append")
    try:
        # Intentar leer el archivo existente
        df_old = pd.read_parquet(file_path)
    except FileNotFoundError:
        # Si el archivo no existe, es la primera carga incremental
        df_old = pd.DataFrame()
        print(f"Primera carga incremental.")
    except Exception as e:
        print(f"Error al leer {file_path}: {e}")
        df_old = pd.DataFrame() # En caso de error, empezamos de cero

    if not df_old.empty:
        # Concatenar el DataFrame antiguo con el nuevo
        df_all = pd.concat([df_old, df_new], ignore_index=True)

        # Eliminar duplicados si se proporciona una columna clave
        if key_col:
            # Asegurarse de que key_col sea una lista para drop_duplicates
            if isinstance(key_col, str):
                key_col = [key_col]
            df_all = df_all.drop_duplicates(subset=key_col, keep="last")
            print(f"[INFO] Duplicados eliminados usando {key_col}.")
        else:
            # Si no hay key_col, eliminar duplicados basados en todas las columnas
            df_all = df_all.drop_duplicates(keep="last")
            print("[INFO] Duplicados eliminados (todas las columnas).")

        # Guardar el DataFrame unificado y sin duplicados
        df_all.to_parquet(file_path, index=False)
        print(f"[INFO] Guardado INCREMENTAL en {file_path}. Nuevos: {len(df_new)}, Total: {len(df_all)}")
    else:
        # Si df_old estaba vacío (primera carga o error previo), simplemente guardar el nuevo df
        df_new.to_parquet(file_path, index=False)
        print(f"[INFO] Guardado inicial (incremental) en {file_path}. Registros: {len(df_new)}")


