from datetime import datetime
from pathlib import Path
import logging

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_BRONZE = BASE_DIR / "data" / "bronze"
DATA_SILVER = BASE_DIR / "data" / "silver"

API_BASE_URL = "https://api.spacexdata.com/v4"

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


def get_partition_path(layer, endpoint_name: str, incremental: bool):
    """
    Genera la ruta del directorio para guardar datos en el data lake.

    Args:
        layer (str): Capa del data lake ("bronze", "silver", "gold").
        endpoint_name (str): El nombre del endpoint de la API.
        incremental (bool, opcional): Si es True, particiona por fecha.
        update (bool, opcional): Si True, indica que no hay actualización.

    Returns:
        tuple[str | None, bool]: La ruta del directorio y un flag update.
    """
    global LAST_DATE
    now = datetime.utcnow()

    # Incremental
    if incremental:
        if now > LAST_DATE:
            path = DATA_BRONZE / endpoint_name / "incremental" / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
            LAST_DATE = now
            return str(path), False
        else:
            print(f"[INFO] No hay datos nuevos para carga incremental de {endpoint_name}. Última carga: {LAST_DATE}")
            return None, True

    # Full
    else:
        if layer == "silver":
            path = DATA_SILVER / endpoint_name / "full" / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        else:
            path = DATA_BRONZE / endpoint_name / "full" / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
    return str(path), False