#!/usr/bin/env python3
"""
FastAPI Serving Layer pour les données de surveillance épidémiologique
Expose les données traitées via des APIs REST optimisées pour Grafana
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from pydantic import BaseModel
import logging
from functools import lru_cache

# Configuration
app = FastAPI(
    title="COVID Surveillance API",
    description="API pour les données de surveillance épidémiologique",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configuration CORS pour Grafana
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En production, spécifier les domaines autorisés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration des chemins
DATA_DIR = Path("data/processed")
LATEST_DATA_FILE = DATA_DIR / "latest_covid_processed.parquet"

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# MODÈLES PYDANTIC
# =============================================================================

class HealthCheck(BaseModel):
    """Modèle pour le health check"""
    status: str
    timestamp: datetime
    data_freshness: Optional[str] = None

class CountryData(BaseModel):
    """Modèle pour les données d'un pays"""
    iso_code: str
    location: str
    date: date
    new_cases: Optional[float] = None
    total_cases: Optional[float] = None
    new_deaths: Optional[float] = None
    total_deaths: Optional[float] = None
    incidence_rate_100k: Optional[float] = None
    case_fatality_rate: Optional[float] = None
    vaccination_rate: Optional[float] = None

class TimeSeriesPoint(BaseModel):
    """Modèle pour les points de série temporelle"""
    timestamp: int  # Unix timestamp en millisecondes (format Grafana)
    value: float

class GrafanaMetric(BaseModel):
    """Modèle pour les métriques Grafana"""
    target: str
    datapoints: List[List[float]]  # Format: [[value, timestamp], ...]

class SummaryStats(BaseModel):
    """Modèle pour les statistiques résumées"""
    country: str
    total_cases: Optional[int] = None
    total_deaths: Optional[int] = None
    current_incidence: Optional[float] = None
    trend_7d: Optional[str] = None  # "increasing", "decreasing", "stable"
    vaccination_rate: Optional[float] = None

# =============================================================================
# CACHE ET HELPERS
# =============================================================================

@lru_cache(maxsize=1)
def load_latest_data() -> pd.DataFrame:
    """Charge les dernières données avec cache"""
    try:
        if not LATEST_DATA_FILE.exists():
            raise HTTPException(status_code=503, detail="Données non disponibles")
        
        df = pd.read_parquet(LATEST_DATA_FILE)
        df['date'] = pd.to_datetime(df['date'])
        logger.info(f"Données chargées: {len(df)} lignes, {df['date'].max()}")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        raise HTTPException(status_code=503, detail=f"Erreur de chargement: {str(e)}")

def calculate_trend(values: pd.Series) -> str:
    """Calcule la tendance sur une série de valeurs"""
    if len(values) < 7:
        return "insufficient_data"
    
    recent = values.tail(7).mean()
    previous = values.iloc[-14:-7].mean() if len(values) >= 14 else values.head(7).mean()
    
    if pd.isna(recent) or pd.isna(previous):
        return "no_data"
    
    change = (recent - previous) / previous if previous > 0 else 0
    
    if change > 0.1:
        return "increasing"
    elif change < -0.1:
        return "decreasing"
    else:
        return "stable"

def to_grafana_timestamp(dt: pd.Timestamp) -> int:
    """Convertit un timestamp pandas en timestamp Grafana (millisecondes)"""
    return int(dt.timestamp() * 1000)

# =============================================================================
# ENDPOINTS PRINCIPAUX
# =============================================================================

@app.get("/", response_model=HealthCheck)
async def health_check():
    """Health check avec informations sur la fraîcheur des données"""
    try:
        df = load_latest_data()
        latest_date = df['date'].max()
        days_old = (datetime.now().date() - latest_date.date()).days
        
        return HealthCheck(
            status="healthy",
            timestamp=datetime.now(),
            data_freshness=f"{days_old} jours"
        )
    except Exception as e:
        return HealthCheck(
            status="unhealthy",
            timestamp=datetime.now(),
            data_freshness="unavailable"
        )

@app.get("/countries", response_model=List[Dict[str, str]])
async def get_countries():
    """Liste des pays disponibles"""
    try:
        df = load_latest_data()
        countries = df[['iso_code', 'location']].drop_duplicates().sort_values('location')
        return countries.to_dict('records')
    except Exception as e:
        logger.error(f"Erreur get_countries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/summary", response_model=List[SummaryStats])
async def get_summary_stats():
    """Statistiques résumées pour tous les pays"""
    try:
        df = load_latest_data()
        
        summaries = []
        for country in df['iso_code'].unique():
            country_data = df[df['iso_code'] == country].sort_values('date')
            latest = country_data.iloc[-1] if len(country_data) > 0 else None
            
            if latest is not None:
                trend = calculate_trend(country_data['new_cases']) if 'new_cases' in country_data.columns else "no_data"
                
                summary = SummaryStats(
                    country=latest['location'],
                    total_cases=int(latest.get('total_cases', 0)) if pd.notna(latest.get('total_cases')) else None,
                    total_deaths=int(latest.get('total_deaths', 0)) if pd.notna(latest.get('total_deaths')) else None,
                    current_incidence=round(latest.get('incidence_rate_100k', 0), 2) if pd.notna(latest.get('incidence_rate_100k')) else None,
                    trend_7d=trend,
                    vaccination_rate=round(latest.get('vaccination_rate', 0), 1) if pd.notna(latest.get('vaccination_rate')) else None
                )
                summaries.append(summary)
        
        return summaries
    except Exception as e:
        logger.error(f"Erreur get_summary_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# ENDPOINTS POUR GRAFANA
# =============================================================================

@app.post("/grafana/search")
async def grafana_search():
    """Endpoint pour la découverte des métriques dans Grafana"""
    metrics = [
        "new_cases",
        "total_cases", 
        "new_deaths",
        "total_deaths",
        "incidence_rate_100k",
        "incidence_rate_100k_7day_avg",
        "case_fatality_rate",
        "vaccination_rate"
    ]
    return metrics

@app.post("/grafana/query")
async def grafana_query(request: dict):
    """Endpoint principal pour les requêtes Grafana"""
    try:
        df = load_latest_data()
        targets = request.get('targets', [])
        range_from = request.get('range', {}).get('from')
        range_to = request.get('range', {}).get('to')
        
        # Filtrage temporel si spécifié
        if range_from and range_to:
            start_date = pd.to_datetime(range_from)
            end_date = pd.to_datetime(range_to)
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        
        results = []
        
        for target in targets:
            target_str = target.get('target', '')
            
            # Parse du format "metric:country" ou juste "metric"
            if ':' in target_str:
                metric, country = target_str.split(':', 1)
                country_filter = df['iso_code'] == country
            else:
                metric = target_str
                country_filter = df['iso_code'] == 'OWID_WRL'  # Données mondiales par défaut
            
            if metric in df.columns:
                filtered_df = df[country_filter].sort_values('date')
                
                # Préparation des datapoints pour Grafana
                datapoints = []
                for _, row in filtered_df.iterrows():
                    value = row[metric]
                    if pd.notna(value):
                        timestamp = to_grafana_timestamp(row['date'])
                        datapoints.append([float(value), timestamp])
                
                results.append({
                    'target': target_str,
                    'datapoints': datapoints
                })
        
        return results
    
    except Exception as e:
        logger.error(f"Erreur grafana_query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/timeseries/{metric}")
async def get_timeseries(
    metric: str,
    countries: Optional[List[str]] = Query(default=['OWID_WRL']),
    days_back: Optional[int] = Query(default=90, ge=1, le=365)
):
    """Endpoint pour récupérer des séries temporelles"""
    try:
        df = load_latest_data()
        
        # Filtrage temporel
        cutoff_date = datetime.now() - timedelta(days=days_back)
        df = df[df['date'] >= cutoff_date]
        
        # Vérification de l'existence de la métrique
        if metric not in df.columns:
            available_metrics = [col for col in df.columns if col not in ['iso_code', 'location', 'date']]
            raise HTTPException(
                status_code=400, 
                detail=f"Métrique '{metric}' non disponible. Métriques disponibles: {available_metrics}"
            )
        
        results = {}
        for country in countries:
            country_data = df[df['iso_code'] == country].sort_values('date')
            
            if len(country_data) > 0:
                timeseries = []
                for _, row in country_data.iterrows():
                    if pd.notna(row[metric]):
                        timeseries.append({
                            'timestamp': to_grafana_timestamp(row['date']),
                            'value': float(row[metric])
                        })
                
                results[country] = timeseries
        
        return results
    
    except Exception as e:
        logger.error(f"Erreur get_timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# ENDPOINTS SPÉCIALISÉS
# =============================================================================

@app.get("/country/{country_code}")
async def get_country_data(
    country_code: str,
    days_back: Optional[int] = Query(default=30, ge=1, le=365)
):
    """Données complètes pour un pays spécifique"""
    try:
        df = load_latest_data()
        
        # Vérification de l'existence du pays
        if country_code not in df['iso_code'].values:
            available_countries = df['iso_code'].unique().tolist()
            raise HTTPException(
                status_code=404,
                detail=f"Pays '{country_code}' non trouvé. Pays disponibles: {available_countries}"
            )
        
        # Filtrage
        cutoff_date = datetime.now() - timedelta(days=days_back)
        country_data = df[
            (df['iso_code'] == country_code) & 
            (df['date'] >= cutoff_date)
        ].sort_values('date')
        
        # Conversion en format API
        results = []
        for _, row in country_data.iterrows():
            results.append(CountryData(
                iso_code=row['iso_code'],
                location=row['location'],
                date=row['date'].date(),
                new_cases=row.get('new_cases'),
                total_cases=row.get('total_cases'),
                new_deaths=row.get('new_deaths'),
                total_deaths=row.get('total_deaths'),
                incidence_rate_100k=row.get('incidence_rate_100k'),
                case_fatality_rate=row.get('case_fatality_rate'),
                vaccination_rate=row.get('vaccination_rate')
            ))
        
        return results
    
    except Exception as e:
        logger.error(f"Erreur get_country_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts")
async def get_alerts():
    """Endpoint pour les alertes basées sur des seuils"""
    try:
        df = load_latest_data()
        
        # Seuils d'alerte
        HIGH_INCIDENCE_THRESHOLD = 100  # cas pour 100k habitants
        HIGH_CFR_THRESHOLD = 5  # Case Fatality Rate en %
        
        alerts = []
        
        for country in df['iso_code'].unique():
            latest_data = df[df['iso_code'] == country].sort_values('date').iloc[-1]
            
            # Alerte incidence élevée
            if pd.notna(latest_data.get('incidence_rate_100k')) and latest_data['incidence_rate_100k'] > HIGH_INCIDENCE_THRESHOLD:
                alerts.append({
                    'country': latest_data['location'],
                    'type': 'high_incidence',
                    'value': latest_data['incidence_rate_100k'],
                    'threshold': HIGH_INCIDENCE_THRESHOLD,
                    'date': latest_data['date'].isoformat()
                })
            
            # Alerte CFR élevé
            if pd.notna(latest_data.get('case_fatality_rate')) and latest_data['case_fatality_rate'] > HIGH_CFR_THRESHOLD:
                alerts.append({
                    'country': latest_data['location'],
                    'type': 'high_cfr',
                    'value': latest_data['case_fatality_rate'],
                    'threshold': HIGH_CFR_THRESHOLD,
                    'date': latest_data['date'].isoformat()
                })
        
        return alerts
    
    except Exception as e:
        logger.error(f"Erreur get_alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# LANCEMENT DE L'APPLICATION
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")