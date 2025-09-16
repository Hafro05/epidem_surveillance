#!/usr/bin/env python3
"""
Script d'ingestion pour la surveillance épidémiologique
Source: Our World in Data COVID-19 dataset
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import logging
import os

# Configuration
RAW_DATA_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" 
PROCESSED_DIR = DATA_DIR / "processed"

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Crée la structure des dossiers si elle n'existe pas"""
    for directory in [RAW_DIR, PROCESSED_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    logger.info("Structure des dossiers créée")

def download_data():
    """Télécharge les données depuis Our World in Data"""
    try:
        logger.info(f"Début du téléchargement depuis {RAW_DATA_URL}")
        
        # Téléchargement avec timeout
        response = requests.get(RAW_DATA_URL, timeout=30)
        response.raise_for_status()
        
        # Génération du nom de fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"owid_covid_data_{timestamp}.csv"
        filepath = RAW_DIR / filename
        
        # Sauvegarde
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        logger.info(f"Données téléchargées avec succès : {filepath}")
        logger.info(f"Taille du fichier : {filepath.stat().st_size / (1024*1024):.2f} MB")
        
        return filepath
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")
        raise

def validate_data(filepath):
    """Valide basiquement les données téléchargées"""
    try:
        logger.info("Début de la validation des données")
        
        # Lecture du fichier
        df = pd.read_csv(filepath)
        
        # Vérifications de base
        logger.info(f"Nombre de lignes : {len(df):,}")
        logger.info(f"Nombre de colonnes : {len(df.columns)}")
        logger.info(f"Période couverte : {df['date'].min()} à {df['date'].max()}")
        logger.info(f"Nombre de pays : {df['location'].nunique()}")
        
        # Vérifications critiques
        required_columns = ['date', 'location', 'total_cases', 'new_cases']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colonnes manquantes : {missing_columns}")
        
        # Vérification que nous avons des données récentes (dernières 7 jours)
        df['date'] = pd.to_datetime(df['date'])
        latest_date = df['date'].max()
        days_old = (datetime.now().date() - latest_date.date()).days
        
        if days_old > 7:
            logger.warning(f"Données anciennes : {days_old} jours")
        else:
            logger.info(f"Données récentes : {days_old} jours d'ancienneté")
        
        logger.info("✅ Validation des données réussie")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation : {e}")
        raise

def create_latest_symlink(filepath):
    """Crée un lien symbolique vers le fichier le plus récent"""
    try:
        latest_link = RAW_DIR / "latest_owid_covid_data.csv"
        
        # Supprime l'ancien lien s'il existe
        if latest_link.exists():
            latest_link.unlink()
        
        # Crée le nouveau lien
        latest_link.symlink_to(filepath.name)
        logger.info(f"Lien symbolique créé : {latest_link}")
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du lien symbolique : {e}")
        # Non critique, on continue

def main():
    """Fonction principale d'ingestion"""
    try:
        logger.info("🚀 Début du processus d'ingestion")
        
        # 1. Configuration des dossiers
        setup_directories()
        
        # 2. Téléchargement
        filepath = download_data()
        
        # 3. Validation
        validate_data(filepath)
        
        # 4. Création du lien symbolique
        create_latest_symlink(filepath)
        
        logger.info("✅ Processus d'ingestion terminé avec succès")
        
    except Exception as e:
        logger.error(f"❌ Échec du processus d'ingestion : {e}")
        raise

if __name__ == "__main__":
    main()#!/usr/bin/env python3
"""
Script d'ingestion pour la surveillance épidémiologique
Source: Our World in Data COVID-19 dataset
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import logging
import os

# Configuration
RAW_DATA_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" 
PROCESSED_DIR = DATA_DIR / "processed"

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Crée la structure des dossiers si elle n'existe pas"""
    for directory in [RAW_DIR, PROCESSED_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    logger.info("Structure des dossiers créée")

def download_data():
    """Télécharge les données depuis Our World in Data"""
    try:
        logger.info(f"Début du téléchargement depuis {RAW_DATA_URL}")
        
        # Téléchargement avec timeout
        response = requests.get(RAW_DATA_URL, timeout=30)
        response.raise_for_status()
        
        # Génération du nom de fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"owid_covid_data_{timestamp}.csv"
        filepath = RAW_DIR / filename
        
        # Sauvegarde
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        logger.info(f"Données téléchargées avec succès : {filepath}")
        logger.info(f"Taille du fichier : {filepath.stat().st_size / (1024*1024):.2f} MB")
        
        return filepath
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")
        raise

def validate_data(filepath):
    """Valide basiquement les données téléchargées"""
    try:
        logger.info("Début de la validation des données")
        
        # Lecture du fichier
        df = pd.read_csv(filepath)
        
        # Vérifications de base
        logger.info(f"Nombre de lignes : {len(df):,}")
        logger.info(f"Nombre de colonnes : {len(df.columns)}")
        logger.info(f"Période couverte : {df['date'].min()} à {df['date'].max()}")
        logger.info(f"Nombre de pays : {df['location'].nunique()}")
        
        # Vérifications critiques
        required_columns = ['date', 'location', 'total_cases', 'new_cases']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colonnes manquantes : {missing_columns}")
        
        # Vérification que nous avons des données récentes (dernières 7 jours)
        df['date'] = pd.to_datetime(df['date'])
        latest_date = df['date'].max()
        days_old = (datetime.now().date() - latest_date.date()).days
        
        if days_old > 7:
            logger.warning(f"Données anciennes : {days_old} jours")
        else:
            logger.info(f"Données récentes : {days_old} jours d'ancienneté")
        
        logger.info("✅ Validation des données réussie")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation : {e}")
        raise

def create_latest_symlink(filepath):
    """Crée un lien symbolique vers le fichier le plus récent"""
    try:
        latest_link = RAW_DIR / "latest_owid_covid_data.csv"
        
        # Supprime l'ancien lien s'il existe
        if latest_link.exists():
            latest_link.unlink()
        
        # Crée le nouveau lien
        latest_link.symlink_to(filepath.name)
        logger.info(f"Lien symbolique créé : {latest_link}")
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du lien symbolique : {e}")
        # Non critique, on continue

def main():
    """Fonction principale d'ingestion"""
    try:
        logger.info("🚀 Début du processus d'ingestion")
        
        # 1. Configuration des dossiers
        setup_directories()
        
        # 2. Téléchargement
        filepath = download_data()
        
        # 3. Validation
        validate_data(filepath)
        
        # 4. Création du lien symbolique
        create_latest_symlink(filepath)
        
        logger.info("✅ Processus d'ingestion terminé avec succès")
        
    except Exception as e:
        logger.error(f"❌ Échec du processus d'ingestion : {e}")
        raise

if __name__ == "__main__":
    main()