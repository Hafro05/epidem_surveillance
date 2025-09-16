#!/bin/bash
# etl-cron.sh - Script d'exécution périodique de l'ETL

set -e

# Configuration des logs
LOG_FILE="/app/logs/etl-$(date +%Y%m%d).log"
ERROR_LOG_FILE="/app/logs/etl-error-$(date +%Y%m%d).log"

# Fonction de logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$LOG_FILE" >> "$ERROR_LOG_FILE"
}

# Début du processus ETL
log "🔄 Début de l'exécution ETL automatique"

# Vérification des prérequis
if [ ! -f "/app/data/processed/latest_covid_processed.parquet" ]; then
    log_error "Fichier de données Parquet non trouvé"
    exit 1
fi

# Test de connexion PostgreSQL
if ! pg_isready -h postgres-analytics -p 5432 -U analytics -d covid_analytics -t 10; then
    log_error "PostgreSQL non accessible"
    exit 1
fi

# Test de connexion Redis (optionnel)
if ! redis-cli -h redis -p 6379 ping > /dev/null 2>&1; then
    log "⚠️ Redis non accessible (continuez sans cache)"
fi

# Exécution de l'ETL
log "📊 Exécution du processus ETL..."
cd /app

if python etl_postgres.py; then
    log "✅ ETL terminé avec succès"
    
    # Nettoyage des anciens logs (garder 7 jours)
    find /app/logs -name "etl-*.log" -mtime +7 -delete 2>/dev/null || true
    
    exit 0
else
    exit_code=$?
    log_error "ETL échoué avec le code $exit_code"
    
    # En cas d'erreur, envoyer une notification (optionnel)
    # curl -X POST "http://covid-api:8000/internal/etl-error" \
    #      -H "Content-Type: application/json" \
    #      -d '{"error_code": '$exit_code', "timestamp": "'$(date -Iseconds)'"}' || true
    
    exit $exit_code
fi
