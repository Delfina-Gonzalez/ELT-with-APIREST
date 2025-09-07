from datetime import datetime
from pathlib import Path
import logging

# import configparser

# Esto funciona cuando el script se ejecuta directamente
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_BRONZE = BASE_DIR / "data" / "bronze"
DATA_SILVER = BASE_DIR / "data" / "silver"

API_BASE_URL = "https://api.spacexdata.com/v4"
# Obtener la clave del archivo .config. No se implementó porque la API no lo requiere.
# "API_KEY": config.get("API_KEYS", "spacex_api_key", fallback=None)

LAST_DATE = datetime.strptime("1900-06-01", "%Y-%m-%d")

ENDPOINTS = {
    "latest_launch": "/launches/latest",
    "upcoming_launches": "/launches/upcoming",
    "rockets": "/rockets",
    "dragons": "/dragons"
}

def setup_logger():
    """
    Configura un logger con un formato estándar para el proyecto.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("ProyectoELT")

def get_partition_path(layer, endpoint_name: str, incremental: bool) -> str:
    """
    Genera la ruta del directorio para guardar datos en el data lake.

    Args:
        endpoint_name (str): El nombre del endpoint de la API.
        incremental (bool, opcional): Si es True, particiona por fecha.
            Por defecto es True.

    Returns:
        str: La ruta de archivo completa para guardar los datos.
    """
    global LAST_DATE
    now = datetime.utcnow()
    
    # Si la carga es incremental, se almacena en el subdirectorio 'incremental'
    if incremental and now > LAST_DATE:
        path = DATA_BRONZE / endpoint_name / "incremental" / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        LAST_DATE = now
        return str(path)
    elif not incremental:
        path = DATA_BRONZE / endpoint_name / "full" / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        return str(path)
    else:
        print("No hay datos nuevos para carga incremental. Fecha de última carga:", LAST_DATE)
        return ""