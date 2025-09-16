from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import pandas as pd
import requests
import logging
import sys

# Configuration du DAG
DAG_ID = 'covid_surveillance_pipeline'
SCHEDULE_INTERVAL = '0 6 * * *'  # Tous les jours à 6h du matin
START_DATE = days_ago(1)

# Configuration des chemins
BASE_DIR = Path('/opt/airflow/data')  # Chemin dans le container Airflow
RAW_DIR = BASE_DIR / 'raw'
PROCESSED_DIR = BASE_DIR / 'processed'

# Configuration des données
RAW_DATA_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Arguments par défaut pour les tâches
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'email': ['admin@company.com'],  # À remplacer par votre email
}

def setup_directories():
    """Crée la structure des dossiers nécessaire"""
    for directory in [RAW_DIR, PROCESSED_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    logging.info("Structure des dossiers créée/vérifiée")

def check_data_source_availability():
    """Vérifie que la source de données est accessible"""
    try:
        response = requests.head(RAW_DATA_URL, timeout=10)
        if response.status_code == 200:
            logging.info("✅ Source de données accessible")
            return True
        else:
            raise Exception(f"Source inaccessible, status code: {response.status_code}")
    except Exception as e:
        logging.error(f"❌ Source de données inaccessible: {e}")
        raise

def download_covid_data():
    """Télécharge les données COVID depuis Our World in Data"""
    try:
        logging.info(f"Début du téléchargement depuis {RAW_DATA_URL}")
        
        # Téléchargement
        response = requests.get(RAW_DATA_URL, timeout=300)  # 5 min timeout
        response.raise_for_status()
        
        # Génération du nom de fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"owid_covid_data_{timestamp}.csv"
        filepath = RAW_DIR / filename
        
        # Sauvegarde
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Création du lien symbolique
        latest_link = RAW_DIR / "latest_owid_covid_data.csv"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(filename)
        
        file_size_mb = filepath.stat().st_size / (1024*1024)
        logging.info(f"✅ Données téléchargées: {filepath} ({file_size_mb:.2f} MB)")
        
        return str(filepath)
        
    except Exception as e:
        logging.error(f"❌ Erreur lors du téléchargement: {e}")
        raise

def validate_raw_data():
    """Valide les données téléchargées"""
    try:
        latest_file = RAW_DIR / "latest_owid_covid_data.csv"
        
        if not latest_file.exists():
            raise FileNotFoundError("Fichier de données non trouvé")
        
        # Lecture et validation
        df = pd.read_csv(latest_file)
        
        # Vérifications critiques
        required_columns = ['date', 'location', 'iso_code', 'total_cases', 'new_cases']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colonnes manquantes: {missing_columns}")
        
        # Vérification de la fraîcheur des données
        df['date'] = pd.to_datetime(df['date'])
        latest_date = df['date'].max()
        days_old = (datetime.now().date() - latest_date.date()).days
        
        if days_old > 7:
            logging.warning(f"⚠️ Données anciennes: {days_old} jours")
        
        # Statistiques de validation
        stats = {
            'rows': len(df),
            'countries': df['location'].nunique(),
            'date_range': f"{df['date'].min().date()} à {df['date'].max().date()}",
            'data_age_days': days_old
        }
        
        logging.info(f"✅ Validation réussie: {stats}")
        return stats
        
    except Exception as e:
        logging.error(f"❌ Échec de la validation: {e}")
        raise

def transform_data():
    """Transforme les données brutes"""
    try:
        sys.path.append("/opt/airflow/")
        from scripts.transformation_script import main as transform_main
        
        # Exécute le script de transformation
        df, quality_report = transform_main()
        
        # Log des résultats
        logging.info(f"✅ Transformation terminée:")
        logging.info(f"  - Lignes traitées: {len(df):,}")
        logging.info(f"  - Pays: {df['iso_code'].nunique()}")
        logging.info(f"  - Colonnes: {len(df.columns)}")
        
        return quality_report
        
    except Exception as e:
        logging.error(f"❌ Erreur lors de la transformation: {e}")
        raise

def data_quality_checks():
    """Effectue des vérifications de qualité sur les données transformées"""
    try:
        latest_file = PROCESSED_DIR / "latest_covid_processed.parquet"
        
        if not latest_file.exists():
            raise FileNotFoundError("Fichier de données transformées non trouvé")
        
        df = pd.read_parquet(latest_file)
        
        # Tests de qualité
        quality_issues = []
        
        # 1. Vérification des valeurs négatives dans les nouveaux cas
        negative_cases = df[df['new_cases'] < 0]
        if len(negative_cases) > 0:
            quality_issues.append(f"Valeurs négatives dans new_cases: {len(negative_cases)} lignes")
        
        # 2. Vérification de la cohérence des données cumulatives
        for country in df['iso_code'].unique():
            country_data = df[df['iso_code'] == country].sort_values('date')
            if 'total_cases' in country_data.columns:
                # Vérifier que les totaux sont croissants (ou stables)
                decreasing = country_data['total_cases'].diff() < -1000  # Tolérance pour corrections
                if decreasing.any():
                    quality_issues.append(f"Totaux décroissants détectés pour {country}")
        
        # 3. Vérification des taux calculés
        if 'incidence_rate_100k' in df.columns:
            extreme_rates = df[df['incidence_rate_100k'] > 1000]  # Plus de 1000 cas / 100k habitants
            if len(extreme_rates) > 0:
                quality_issues.append(f"Taux d'incidence extrêmes: {len(extreme_rates)} observations")
        
        # Résultats
        if quality_issues:
            logging.warning(f"⚠️ Problèmes de qualité détectés:")
            for issue in quality_issues:
                logging.warning(f"  - {issue}")
        else:
            logging.info("✅ Tous les tests de qualité sont passés")
        
        quality_summary = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'quality_issues': quality_issues,
            'status': 'WARNING' if quality_issues else 'OK'
        }
        
        return quality_summary
        
    except Exception as e:
        logging.error(f"❌ Erreur lors des vérifications qualité: {e}")
        raise

def send_success_notification(**context):
    """Envoie une notification de succès"""
    execution_date = context['ds']
    task_instance = context['task_instance']
    
    # Récupération des statistiques des tâches précédentes
    validation_stats = task_instance.xcom_pull(task_ids='ingestion_group.validate_raw_data')
    quality_report = task_instance.xcom_pull(task_ids='data_quality_checks')
    
    message = f"""
    ✅ Pipeline de surveillance épidémiologique exécuté avec succès
    
    📅 Date d'exécution: {execution_date}
    📊 Données traitées: {validation_stats.get('rows', 'N/A')} lignes
    🌍 Pays couverts: {validation_stats.get('countries', 'N/A')}
    📈 Période: {validation_stats.get('date_range', 'N/A')}
    
    🔍 Statut qualité: {quality_report.get('status', 'N/A')}
    
    Les données sont disponibles dans: {PROCESSED_DIR}/latest_covid_processed.parquet
    """
    
    logging.info("📧 Notification de succès envoyée")
    return message

# Création du DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Pipeline de surveillance épidémiologique COVID-19',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,  # Ne pas exécuter les runs manqués
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['epidemiologie', 'covid', 'data-engineering'],
)

# === DÉFINITION DES TÂCHES ===

# Tâche de préparation
setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

# Groupe de tâches d'ingestion
with TaskGroup('ingestion_group', dag=dag) as ingestion_group:
    
    check_source_task = PythonOperator(
        task_id='check_data_source',
        python_callable=check_data_source_availability,
        dag = dag
    )
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        dag = dag
    )
    
    validate_task = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_raw_data,
        dag = dag
    )
    
    check_source_task >> download_task >> validate_task

# Tâche de transformation
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Tâche de vérification qualité
quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag,
)

# Tâche de notification
notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Tâche de nettoyage (optionnelle)
cleanup_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command=f"""
    # Supprime les fichiers raw de plus de 7 jours
    find {RAW_DIR} -name "owid_covid_data_*.csv" -mtime +7 -delete || true
    # Supprime les fichiers processed de plus de 30 jours  
    find {PROCESSED_DIR} -name "covid_processed_*.parquet" -mtime +30 -delete || true
    echo "Nettoyage terminé"
    """,
    dag=dag,
)

# === DÉFINITION DES DÉPENDANCES ===

setup_task >> ingestion_group >> transform_task >> quality_check_task >> notification_task >> cleanup_task