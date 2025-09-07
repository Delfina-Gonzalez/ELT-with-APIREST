import requests
import pandas as pd
from requests.exceptions import HTTPError
from json.decoder import JSONDecodeError
from src.config import API_BASE_URL, ENDPOINTS

def get_json_from_api(endpoint: str) -> dict:
    """Obtiene datos crudos desde un endpoint de la SpaceX API."""
    url = f"{API_BASE_URL}{ENDPOINTS[endpoint]}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def fetch_data(endpoint: str) -> pd.DataFrame:
    """Convierte respuesta JSON a DataFrame plano."""
    try:
        data = get_json_from_api(endpoint)
        if isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            df = pd.json_normalize([data])
        return df
    except (HTTPError, JSONDecodeError) as e:
        print(f"❌ Error al obtener datos de '{endpoint}': {e}")
        return pd.DataFrame()

    
