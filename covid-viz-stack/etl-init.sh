#!/bin/bash
# etl-init.sh - Script d'initialisation du service ETL

set -e

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log "🚀 Initialisation du service ETL"

# Création des dossiers de logs
mkdir -p /app/logs
chmod 755 /app/logs

# Attendre que PostgreSQL soit prêt
log "⏳ Attente de PostgreSQL..."
for i in {1..30}; do
    if pg_isready -h postgres-analytics -p 5432 -U analytics -d covid_analytics -t 5; then
        log "✅ PostgreSQL prêt"
        break
    fi
    if [ $i -eq 30 ]; then
        log "❌ Timeout: PostgreSQL non accessible après 150s"
        exit 1
    fi
    sleep 5
done

# Test initial de l'ETL si les données sont disponibles
if [ -f "data/processed/latest_covid_processed.parquet" ]; then
    log "📊 Exécution ETL initiale..."
    if python etl_postgres.py; then
        log "✅ ETL initial terminé avec succès"
    else
        log "⚠️ ETL initial échoué (continuez avec cron)"
    fi
else
    log "⚠️ Pas de données Parquet disponibles pour l'ETL initial"
fi

# Démarrage du service cron
log "⏰ Démarrage du service cron..."
service cron start

# Vérification que cron fonctionne
if ps aux | grep -q '[c]ron'; then
    log "✅ Service cron démarré avec succès"
else
    log "❌ Erreur: Service cron non démarré"
    exit 1
fi

# Affichage de la configuration cron
log "📅 Configuration cron active:"
crontab -l | grep -v "^#" || log "Aucune tâche cron configurée"

# Log de statut
log "🎯 Service ETL initialisé et prêt"
log "📂 Logs disponibles dans: /app/logs/"
log "🔄 Prochaine exécution: à la minute 15 de chaque heure"

# Maintenir le conteneur en vie et suivre les logs
log "👀 Suivi des logs en temps réel..."
touch /app/logs/etl-$(date +%Y%m%d).log

# Suivi des logs avec rotation automatique
exec tail -F /app/logs/etl-*.log