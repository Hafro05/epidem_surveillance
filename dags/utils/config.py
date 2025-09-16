"""
Configuration centralisée pour les DAGs
"""
from pathlib import Path

# Chemins
BASE_DIR = Path('/opt/airflow/data')
RAW_DIR = BASE_DIR / 'raw'
PROCESSED_DIR = BASE_DIR / 'processed'

# URLs des données
OWID_COVID_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Configuration email
DEFAULT_EMAIL = ['admin@company.com']

# Pays d'intérêt
TARGET_COUNTRIES = [
    'FRA', 'DEU', 'ITA', 'ESP', 'GBR', 'BEL', 'NLD', 'OWID_WRL'
]
