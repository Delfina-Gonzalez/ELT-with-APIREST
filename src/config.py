from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Estructura Data Lake
DATA_BRONZE = BASE_DIR / "data" / "bronze"
DATA_SILVER = BASE_DIR / "data" / "silver"
DATA_GOLD   = BASE_DIR / "data" / "gold"

API_BASE_URL = "https://api.spacexdata.com/v4"

ENDPOINTS = {
    "latest_launch": "/launches/latest",
    "upcoming_launches": "/launches/upcoming",
    "rockets": "/rockets",
    "dragons": "/dragons"
}

def get_partition_path(layer: str, endpoint_name: str, incremental: bool = True) -> str:
    """
    Genera la ruta del directorio para guardar datos en el data lake.
    """
    base = {"bronze": DATA_BRONZE, "silver": DATA_SILVER, "gold": DATA_GOLD}[layer]

    if incremental:
        now = datetime.utcnow()
        path = base / endpoint_name / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        return str(path)

    return str(base / endpoint_name)
