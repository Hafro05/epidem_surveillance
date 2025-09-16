#!/usr/bin/env python3
"""
Script de transformation pour la surveillance épidémiologique
Nettoie, enrichit et optimise les données brutes
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Pays d'intérêt (codes ISO)
TARGET_COUNTRIES = [
    'FRA',  # France
    'DEU',  # Allemagne  
    'ITA',  # Italie
    'ESP',  # Espagne
    'GBR',  # Royaume-Uni
    'BEL',  # Belgique
    'NLD',  # Pays-Bas
    'OWID_WRL'  # Monde (agrégé)
]

# Colonnes d'intérêt
CORE_COLUMNS = [
    'iso_code', 'location', 'date', 'population',
    'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
    'new_vaccinations', 'stringency_index'
]

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_raw_data():
    """Charge les données brutes les plus récentes"""
    try:
        # Cherche le fichier le plus récent ou utilise le lien symbolique
        latest_file = RAW_DIR / "latest_owid_covid_data.csv"
        
        if not latest_file.exists():
            # Fallback : cherche le fichier le plus récent
            raw_files = list(RAW_DIR.glob("owid_covid_data_*.csv"))
            if not raw_files:
                raise FileNotFoundError("Aucun fichier de données brutes trouvé")
            latest_file = max(raw_files, key=lambda x: x.stat().st_mtime)
        
        logger.info(f"Chargement des données depuis : {latest_file}")
        df = pd.read_csv(latest_file)
        
        logger.info(f"Données chargées : {len(df):,} lignes, {len(df.columns)} colonnes")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement : {e}")
        raise

def filter_and_clean(df):
    """Filtre les pays d'intérêt et nettoie les données de base"""
    try:
        logger.info("Début du filtrage et nettoyage")
        
        # 1. Filtrer les pays d'intérêt
        df_filtered = df[df['iso_code'].isin(TARGET_COUNTRIES)].copy()
        logger.info(f"Après filtrage pays : {len(df_filtered):,} lignes")
        
        # 2. Garder seulement les colonnes utiles
        available_columns = [col for col in CORE_COLUMNS if col in df_filtered.columns]
        df_filtered = df_filtered[available_columns]
        logger.info(f"Colonnes conservées : {len(available_columns)}")
        
        # 3. Conversion et nettoyage des dates
        df_filtered['date'] = pd.to_datetime(df_filtered['date'])
        
        # 4. Filtrage temporel (2 dernières années)
        cutoff_date = datetime.now() - timedelta(days=730)
        df_filtered = df_filtered[df_filtered['date'] >= cutoff_date]
        logger.info(f"Après filtrage temporel (depuis {cutoff_date.date()}) : {len(df_filtered):,} lignes")
        
        # 5. Tri par pays et date
        df_filtered = df_filtered.sort_values(['iso_code', 'date']).reset_index(drop=True)
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Erreur lors du filtrage : {e}")
        raise

def handle_missing_values(df):
    """Gestion intelligente des valeurs manquantes"""
    try:
        logger.info("Gestion des valeurs manquantes")
        
        # Log initial des valeurs manquantes
        missing_info = df.isnull().sum()
        logger.info("Valeurs manquantes par colonne :")
        for col, count in missing_info.items():
            if count > 0:
                pct = (count / len(df)) * 100
                logger.info(f"  {col}: {count:,} ({pct:.1f}%)")
        
        # Stratégies par type de colonne
        
        # 1. Colonnes cumulatives : forward fill par pays
        cumulative_cols = ['total_cases', 'total_deaths', 'total_vaccinations', 
                          'people_vaccinated', 'people_fully_vaccinated']
        
        for col in cumulative_cols:
            if col in df.columns:
                df[col] = df.groupby('iso_code')[col].fillna(method='ffill')
        
        # 2. Colonnes quotidiennes : 0 pour les valeurs manquantes
        daily_cols = ['new_cases', 'new_deaths', 'new_vaccinations']
        
        for col in daily_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # 3. Population : forward fill (ne change pas souvent)
        if 'population' in df.columns:
            df['population'] = df.groupby('iso_code')['population'].fillna(method='ffill')
        
        # 4. Stringency index : interpolation
        if 'stringency_index' in df.columns:
            df['stringency_index'] = df.groupby('iso_code')['stringency_index'].fillna(method='ffill')
        
        logger.info("✅ Gestion des valeurs manquantes terminée")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors de la gestion des valeurs manquantes : {e}")
        raise

def calculate_derived_metrics(df):
    """Calcule des métriques dérivées utiles pour l'analyse"""
    try:
        logger.info("Calcul des métriques dérivées")
        
        # 1. Taux d'incidence pour 100k habitants
        if 'new_cases' in df.columns and 'population' in df.columns:
            df['incidence_rate_100k'] = (df['new_cases'] / df['population']) * 100000
        
        # 2. Taux de mortalité pour 100k habitants
        if 'new_deaths' in df.columns and 'population' in df.columns:
            df['death_rate_100k'] = (df['new_deaths'] / df['population']) * 100000
        
        # 3. Case Fatality Rate (CFR)
        if 'total_deaths' in df.columns and 'total_cases' in df.columns:
            df['case_fatality_rate'] = np.where(
                df['total_cases'] > 0,
                (df['total_deaths'] / df['total_cases']) * 100,
                0
            )
        
        # 4. Moyennes mobiles sur 7 jours
        rolling_cols = ['new_cases', 'new_deaths', 'incidence_rate_100k']
        
        for col in rolling_cols:
            if col in df.columns:
                new_col_name = f'{col}_7day_avg'
                df[new_col_name] = df.groupby('iso_code')[col].rolling(
                    window=7, center=True, min_periods=1
                ).mean().reset_index(drop=True)
        
        # 5. Pourcentage de population vaccinée
        if 'people_fully_vaccinated' in df.columns and 'population' in df.columns:
            df['vaccination_rate'] = np.where(
                df['population'] > 0,
                (df['people_fully_vaccinated'] / df['population']) * 100,
                0
            )
        
        logger.info(f"✅ Métriques dérivées calculées. Nouvelles colonnes : {len(df.columns)}")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors du calcul des métriques : {e}")
        raise

def generate_quality_report(df):
    """Génère un rapport de qualité des données"""
    try:
        logger.info("Génération du rapport de qualité")
        
        report = {
            'timestamp': datetime.now(),
            'total_rows': len(df),
            'countries': df['iso_code'].nunique(),
            'date_range': {
                'start': df['date'].min().date(),
                'end': df['date'].max().date(),
                'days': (df['date'].max() - df['date'].min()).days
            },
            'data_completeness': {}
        }
        
        # Complétude par colonne
        for col in df.columns:
            if col not in ['iso_code', 'location', 'date']:
                total_values = len(df)
                non_null_values = df[col].notna().sum()
                completeness = (non_null_values / total_values) * 100
                report['data_completeness'][col] = round(completeness, 1)
        
        # Sauvegarder le rapport
        report_file = PROCESSED_DIR / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Log du résumé
        logger.info(f"Rapport de qualité :")
        logger.info(f"  - Lignes: {report['total_rows']:,}")
        logger.info(f"  - Pays: {report['countries']}")
        logger.info(f"  - Période: {report['date_range']['start']} à {report['date_range']['end']}")
        
        return report
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération du rapport : {e}")
        raise

def save_processed_data(df):
    """Sauvegarde les données traitées en format Parquet"""
    try:
        logger.info("Sauvegarde des données traitées")
        
        # Nom du fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = PROCESSED_DIR / f"covid_processed_{timestamp}.parquet"
        
        # Sauvegarde en Parquet avec compression
        df.to_parquet(
            parquet_file,
            compression='snappy',
            index=False
        )
        
        # Créer un lien symbolique vers le fichier le plus récent
        latest_link = PROCESSED_DIR / "latest_covid_processed.parquet"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(parquet_file.name)
        
        # Statistiques du fichier
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        logger.info(f"✅ Données sauvegardées : {parquet_file}")
        logger.info(f"Taille du fichier : {file_size_mb:.2f} MB")
        logger.info(f"Compression ratio vs CSV : ~70-80%")
        
        return parquet_file
        
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde : {e}")
        raise

def main():
    """Fonction principale de transformation"""
    try:
        logger.info("🔄 Début du processus de transformation")
        
        # 1. Charger les données brutes
        df = load_raw_data()
        
        # 2. Filtrer et nettoyer
        df = filter_and_clean(df)
        
        # 3. Gérer les valeurs manquantes
        df = handle_missing_values(df)
        
        # 4. Calculer les métriques dérivées
        df = calculate_derived_metrics(df)
        
        # 5. Générer le rapport de qualité
        quality_report = generate_quality_report(df)
        
        # 6. Sauvegarder les données traitées
        output_file = save_processed_data(df)
        
        logger.info("✅ Processus de transformation terminé avec succès")
        logger.info(f"Fichier de sortie : {output_file}")
        
        return df, quality_report
        
    except Exception as e:
        logger.error(f"❌ Échec du processus de transformation : {e}")
        raise

if __name__ == "__main__":
    main()