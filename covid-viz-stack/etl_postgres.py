#!/usr/bin/env python3
"""
Service ETL pour alimenter PostgreSQL depuis les fichiers Parquet
Optimisé pour les performances et la cohérence des données analytiques
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, DateTime, Boolean
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import redis
import json
from typing import Optional, Dict, List, Any

# Configuration
DATABASE_URL = "postgresql://analytics:analytics123@postgres-analytics:5432/covid_analytics"
REDIS_URL = "redis://redis:6379/0"
DATA_DIR = Path("data/processed")
LATEST_DATA_FILE = DATA_DIR / "latest_covid_processed.parquet"

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION BASE DE DONNÉES
# =============================================================================

def create_tables(engine):
    """Crée les tables analytiques dans PostgreSQL"""
    
    metadata = MetaData()
    
    # Table principale des données COVID
    covid_data = Table(
        'covid_daily_data',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('iso_code', String(10), nullable=False, index=True),
        Column('location', String(100), nullable=False),
        Column('date', Date, nullable=False, index=True),
        Column('population', Float),
        
        # Données brutes
        Column('total_cases', Float),
        Column('new_cases', Float),
        Column('total_deaths', Float),
        Column('new_deaths', Float),
        Column('total_vaccinations', Float),
        Column('people_vaccinated', Float),
        Column('people_fully_vaccinated', Float),
        Column('new_vaccinations', Float),
        Column('stringency_index', Float),
        
        # Métriques calculées
        Column('incidence_rate_100k', Float),
        Column('death_rate_100k', Float),
        Column('case_fatality_rate', Float),
        Column('vaccination_rate', Float),
        Column('new_cases_7day_avg', Float),
        Column('new_deaths_7day_avg', Float),
        Column('incidence_rate_100k_7day_avg', Float),
        
        # Métadonnées
        Column('data_quality_score', Float),
        Column('last_updated', DateTime, default=datetime.utcnow),
        Column('created_at', DateTime, default=datetime.utcnow),
        
        # Index composites pour les performances
        sa.Index('ix_covid_country_date', 'iso_code', 'date'),
        sa.Index('ix_covid_date_incidence', 'date', 'incidence_rate_100k'),
        sa.UniqueConstraint('iso_code', 'date', name='uq_covid_country_date')
    )
    
    # Table des alertes
    alerts_table = Table(
        'covid_alerts',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('country_code', String(10), nullable=False),
        Column('alert_type', String(50), nullable=False),
        Column('alert_date', Date, nullable=False),
        Column('metric_value', Float, nullable=False),
        Column('threshold_value', Float, nullable=False),
        Column('severity', String(20), default='medium'),
        Column('is_active', Boolean, default=True),
        Column('created_at', DateTime, default=datetime.utcnow),
        Column('resolved_at', DateTime),
        
        sa.Index('ix_alerts_country_date', 'country_code', 'alert_date'),
        sa.Index('ix_alerts_active', 'is_active', 'alert_type')
    )
    
    # Table des résumés quotidiens (pour les performances)
    daily_summary = Table(
        'covid_daily_summary',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('summary_date', Date, nullable=False, unique=True),
        Column('total_countries', Integer),
        Column('total_global_cases', Float),
        Column('total_global_deaths', Float),
        Column('new_global_cases', Float),
        Column('new_global_deaths', Float),
        Column('countries_high_incidence', Integer),
        Column('avg_vaccination_rate', Float),
        Column('data_completeness_pct', Float),
        Column('created_at', DateTime, default=datetime.utcnow),
        
        sa.Index('ix_summary_date', 'summary_date')
    )
    
    # Création des tables
    metadata.create_all(engine)
    logger.info("✅ Tables créées/vérifiées dans PostgreSQL")

class CovidETL:
    """Classe principale pour l'ETL des données COVID"""
    
    def __init__(self, database_url: str, redis_url: Optional[str] = None):
        self.engine = create_engine(database_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)
        
        # Cache Redis optionnel
        self.redis_client = None
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url)
                self.redis_client.ping()
                logger.info("✅ Connexion Redis établie")
            except Exception as e:
                logger.warning(f"Redis non disponible: {e}")
        
        # Création des tables
        create_tables(self.engine)
    
    @contextmanager
    def get_session(self):
        """Context manager pour les sessions DB"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def calculate_data_quality_score(self, row: pd.Series) -> float:
        """Calcule un score de qualité des données pour une ligne"""
        score = 100.0
        
        # Pénalités pour valeurs manquantes
        key_fields = ['new_cases', 'total_cases', 'population']
        for field in key_fields:
            if pd.isna(row.get(field)):
                score -= 15
        
        # Pénalités pour valeurs aberrantes
        if not pd.isna(row.get('new_cases')) and row.get('new_cases', 0) < 0:
            score -= 10
        
        if not pd.isna(row.get('case_fatality_rate')) and row.get('case_fatality_rate', 0) > 20:
            score -= 5
        
        return max(0, score)
    
    def detect_alerts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Détecte les alertes basées sur des seuils"""
        alerts = []
        
        # Seuils d'alerte
        THRESHOLDS = {
            'high_incidence': 150,  # cas pour 100k habitants
            'very_high_incidence': 300,
            'high_cfr': 3.0,  # Case Fatality Rate
            'rapid_increase': 50  # % d'augmentation sur 7 jours
        }
        
        latest_date = df['date'].max()
        latest_data = df[df['date'] == latest_date]
        
        for _, row in latest_data.iterrows():
            country = row['iso_code']
            
            # Alerte incidence élevée
            incidence = row.get('incidence_rate_100k', 0)
            if pd.notna(incidence):
                if incidence > THRESHOLDS['very_high_incidence']:
                    alerts.append({
                        'country_code': country,
                        'alert_type': 'very_high_incidence',
                        'alert_date': latest_date.date(),
                        'metric_value': incidence,
                        'threshold_value': THRESHOLDS['very_high_incidence'],
                        'severity': 'high'
                    })
                elif incidence > THRESHOLDS['high_incidence']:
                    alerts.append({
                        'country_code': country,
                        'alert_type': 'high_incidence',
                        'alert_date': latest_date.date(),
                        'metric_value': incidence,
                        'threshold_value': THRESHOLDS['high_incidence'],
                        'severity': 'medium'
                    })
            
            # Alerte CFR élevé
            cfr = row.get('case_fatality_rate', 0)
            if pd.notna(cfr) and cfr > THRESHOLDS['high_cfr']:
                alerts.append({
                    'country_code': country,
                    'alert_type': 'high_cfr',
                    'alert_date': latest_date.date(),
                    'metric_value': cfr,
                    'threshold_value': THRESHOLDS['high_cfr'],
                    'severity': 'medium'
                })
        
        return alerts
    
    def load_data_to_postgres(self, df: pd.DataFrame) -> Dict[str, int]:
        """Charge les données dans PostgreSQL avec upsert"""
        logger.info(f"Début du chargement de {len(df)} lignes vers PostgreSQL")
        
        # Calcul du score de qualité
        df['data_quality_score'] = df.apply(self.calculate_data_quality_score, axis=1)
        df['last_updated'] = datetime.utcnow()
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        with self.get_session() as session:
            try:
                # Préparation des données pour l'insertion
                records = []
                for _, row in df.iterrows():
                    record = {
                        'iso_code': row['iso_code'],
                        'location': row['location'],
                        'date': row['date'].date(),
                        'population': float(row['population']) if pd.notna(row['population']) else None,
                        'total_cases': float(row['total_cases']) if pd.notna(row['total_cases']) else None,
                        'new_cases': float(row['new_cases']) if pd.notna(row['new_cases']) else None,
                        'total_deaths': float(row['total_deaths']) if pd.notna(row['total_deaths']) else None,
                        'new_deaths': float(row['new_deaths']) if pd.notna(row['new_deaths']) else None,
                        'total_vaccinations': float(row.get('total_vaccinations')) if pd.notna(row.get('total_vaccinations')) else None,
                        'people_vaccinated': float(row.get('people_vaccinated')) if pd.notna(row.get('people_vaccinated')) else None,
                        'people_fully_vaccinated': float(row.get('people_fully_vaccinated')) if pd.notna(row.get('people_fully_vaccinated')) else None,
                        'new_vaccinations': float(row.get('new_vaccinations')) if pd.notna(row.get('new_vaccinations')) else None,
                        'stringency_index': float(row.get('stringency_index')) if pd.notna(row.get('stringency_index')) else None,
                        'incidence_rate_100k': float(row.get('incidence_rate_100k')) if pd.notna(row.get('incidence_rate_100k')) else None,
                        'death_rate_100k': float(row.get('death_rate_100k')) if pd.notna(row.get('death_rate_100k')) else None,
                        'case_fatality_rate': float(row.get('case_fatality_rate')) if pd.notna(row.get('case_fatality_rate')) else None,
                        'vaccination_rate': float(row.get('vaccination_rate')) if pd.notna(row.get('vaccination_rate')) else None,
                        'new_cases_7day_avg': float(row.get('new_cases_7day_avg')) if pd.notna(row.get('new_cases_7day_avg')) else None,
                        'new_deaths_7day_avg': float(row.get('new_deaths_7day_avg')) if pd.notna(row.get('new_deaths_7day_avg')) else None,
                        'incidence_rate_100k_7day_avg': float(row.get('incidence_rate_100k_7day_avg')) if pd.notna(row.get('incidence_rate_100k_7day_avg')) else None,
                        'data_quality_score': float(row['data_quality_score']),
                        'last_updated': row['last_updated']
                    }
                    records.append(record)
                
                # Insertion par batch avec upsert PostgreSQL
                from sqlalchemy.dialects.postgresql import insert
                
                # Référence à la table
                metadata = MetaData()
                covid_table = Table('covid_daily_data', metadata, autoload_with=self.engine)
                
                # Upsert en utilisant ON CONFLICT
                stmt = insert(covid_table).values(records)
                upsert_stmt = stmt.on_conflict_do_update(
                    constraint='uq_covid_country_date',
                    set_={
                        col.name: stmt.excluded[col.name] 
                        for col in covid_table.columns 
                        if col.name not in ['id', 'iso_code', 'date', 'created_at']
                    }
                )
                
                result = session.execute(upsert_stmt)
                stats['inserted'] = result.rowcount
                
                logger.info(f"✅ {stats['inserted']} lignes traitées en PostgreSQL")
                
            except Exception as e:
                logger.error(f"❌ Erreur lors du chargement PostgreSQL: {e}")
                stats['errors'] += 1
                raise
        
        return stats
    
    def create_daily_summary(self, df: pd.DataFrame):
        """Crée un résumé quotidien pour les performances"""
        try:
            latest_date = df['date'].max().date()
            
            # Calculs d'agrégation
            world_data = df[df['iso_code'] == 'OWID_WRL']
            latest_world = world_data[world_data['date'] == df['date'].max()]
            
            if len(latest_world) == 0:
                logger.warning("Pas de données mondiales pour le résumé")
                return
            
            world_row = latest_world.iloc[0]
            
            # Métriques globales
            countries_count = df['iso_code'].nunique()
            high_incidence_countries = len(df[
                (df['date'] == df['date'].max()) & 
                (df['incidence_rate_100k'] > 100)
            ])
            
            avg_vaccination = df[
                (df['date'] == df['date'].max()) & 
                (df['vaccination_rate'].notna())
            ]['vaccination_rate'].mean()
            
            data_completeness = (df.notna().sum() / len(df)).mean() * 100
            
            summary = {
                'summary_date': latest_date,
                'total_countries': countries_count,
                'total_global_cases': float(world_row.get('total_cases', 0)) if pd.notna(world_row.get('total_cases')) else None,
                'total_global_deaths': float(world_row.get('total_deaths', 0)) if pd.notna(world_row.get('total_deaths')) else None,
                'new_global_cases': float(world_row.get('new_cases', 0)) if pd.notna(world_row.get('new_cases')) else None,
                'new_global_deaths': float(world_row.get('new_deaths', 0)) if pd.notna(world_row.get('new_deaths')) else None,
                'countries_high_incidence': high_incidence_countries,
                'avg_vaccination_rate': float(avg_vaccination) if pd.notna(avg_vaccination) else None,
                'data_completeness_pct': float(data_completeness),
            }
            
            with self.get_session() as session:
                # Upsert du résumé quotidien
                metadata = MetaData()
                summary_table = Table('covid_daily_summary', metadata, autoload_with=self.engine)
                
                stmt = insert(summary_table).values([summary])
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['summary_date'],  # <- contrainte d’unicité
                    set_={col.name: stmt.excluded[col.name] for col in summary_table.columns if col.name not in ('id', 'summary_date')}
                )
                
                session.execute(upsert_stmt)
                logger.info(f"✅ Résumé quotidien créé pour {latest_date}")
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du résumé: {e}")
    
    def save_alerts(self, alerts: List[Dict[str, Any]]):
        """Sauvegarde les alertes en base"""
        if not alerts:
            logger.info("Aucune alerte à sauvegarder")
            return
        
        try:
            with self.get_session() as session:
                metadata = MetaData()
                alerts_table = Table('covid_alerts', metadata, autoload_with=self.engine)
                
                # Désactiver les anciennes alertes du même type
                for alert in alerts:
                    session.execute(
                        alerts_table.update()
                        .where(
                            (alerts_table.c.country_code == alert['country_code']) &
                            (alerts_table.c.alert_type == alert['alert_type']) &
                            (alerts_table.c.is_active == True)
                        )
                        .values(is_active=False, resolved_at=datetime.utcnow())
                    )
                
                # Insérer les nouvelles alertes
                session.execute(alerts_table.insert().values(alerts))
                logger.info(f"✅ {len(alerts)} alertes sauvegardées")
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la sauvegarde des alertes: {e}")
    
    def update_cache(self, df: pd.DataFrame):
        """Met à jour le cache Redis si disponible"""
        if not self.redis_client:
            return
        
        try:
            # Cache des métriques principales
            latest_date = df['date'].max()
            latest_data = df[df['date'] == latest_date]
            
            # Cache des données par pays
            for _, row in latest_data.iterrows():
                country_key = f"country:{row['iso_code']}:latest"
                country_data = {
                    'location': row['location'],
                    'date': str(row['date'].date()),
                    'total_cases': float(row['total_cases']) if pd.notna(row['total_cases']) else None,
                    'new_cases': float(row['new_cases']) if pd.notna(row['new_cases']) else None,
                    'incidence_rate': float(row.get('incidence_rate_100k')) if pd.notna(row.get('incidence_rate_100k')) else None,
                }
                
                self.redis_client.setex(
                    country_key, 
                    3600,  # TTL: 1 heure
                    json.dumps(country_data, default=str)
                )
            
            # Cache de la liste des pays
            countries = df[['iso_code', 'location']].drop_duplicates().to_dict('records')
            self.redis_client.setex('countries:list', 3600, json.dumps(countries))
            
            # Timestamp de dernière mise à jour
            self.redis_client.setex('data:last_update', 3600, str(datetime.utcnow()))
            
            logger.info("✅ Cache Redis mis à jour")
        
        except Exception as e:
            logger.error(f"⚠️ Erreur mise à jour cache Redis: {e}")
    
    def run_etl(self, force_reload: bool = False) -> Dict[str, Any]:
        """Exécute le processus ETL complet"""
        logger.info("🔄 Début du processus ETL PostgreSQL")
        
        start_time = datetime.utcnow()
        results = {
            'start_time': start_time,
            'status': 'running',
            'stats': {},
            'errors': []
        }
        
        try:
            # 1. Chargement des données Parquet
            if not LATEST_DATA_FILE.exists():
                raise FileNotFoundError(f"Fichier de données non trouvé: {LATEST_DATA_FILE}")
            
            logger.info(f"Chargement depuis: {LATEST_DATA_FILE}")
            df = pd.read_parquet(LATEST_DATA_FILE)
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"Données chargées: {len(df)} lignes, {len(df.columns)} colonnes")
            
            # 2. Chargement vers PostgreSQL
            db_stats = self.load_data_to_postgres(df)
            results['stats']['database'] = db_stats
            
            # 3. Détection des alertes
            alerts = self.detect_alerts(df)
            if alerts:
                self.save_alerts(alerts)
                results['stats']['alerts'] = len(alerts)
            
            # 4. Création du résumé quotidien
            self.create_daily_summary(df)
            
            # 5. Mise à jour du cache
            self.update_cache(df)
            
            # 6. Statistiques finales
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                'end_time': end_time,
                'duration_seconds': duration,
                'status': 'completed',
                'stats': {
                    **results['stats'],
                    'rows_processed': len(df),
                    'countries': df['iso_code'].nunique(),
                    'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}"
                }
            })
            
            logger.info(f"✅ ETL terminé avec succès en {duration:.1f}s")
            return results
        
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                'end_time': end_time,
                'duration_seconds': duration,
                'status': 'failed',
                'errors': [str(e)]
            })
            
            logger.error(f"❌ ETL échoué après {duration:.1f}s: {e}")
            raise

def main():
    """Fonction principale pour l'exécution du script ETL"""
    try:
        etl = CovidETL(DATABASE_URL, REDIS_URL)
        results = etl.run_etl()
        
        print(f"✅ ETL terminé: {results['stats']}")
        
    except Exception as e:
        logger.error(f"❌ Échec du processus ETL: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())