import requests
import pandas as pd
from requests.exceptions import HTTPError
from json.decoder import JSONDecodeError
from src.config import API_BASE_URL, ENDPOINTS

def filter_incremental(df_new: pd.DataFrame, df_old: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """
    Devuelve solo los registros nuevos de df_new comparados con df_old
    en base a una columna temporal (ej: 'date_utc').
    """
    if df_old.empty:
        return df_new

    last_date = df_old[time_col].max()
    df_new[time_col] = pd.to_datetime(df_new[time_col])
    df_filtered = df_new[df_new[time_col] > last_date]

    return df_filtered

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

    
