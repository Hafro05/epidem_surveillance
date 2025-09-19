#!/bin/bash

# Script de déploiement pour la stack de visualisation COVID
# FastAPI + PostgreSQL + Grafana + ETL + Nginx

set -e

# Configuration
PROJECT_NAME="covid-visualization-stack"
GRAFANA_ADMIN_PASSWORD="admin123"
POSTGRES_PASSWORD="analytics123"

# Couleurs pour les messages
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}"
    echo "════════════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "════════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
}

print_step() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Vérification des prérequis
check_prerequisites() {
    print_header "Vérification des prérequis"
    
    # Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker n'est pas installé"
        exit 1
    fi
    print_step "Docker trouvé: $(docker --version)"
    
    # Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose n'est pas installé"
        exit 1
    fi
    print_step "Docker Compose trouvé"
    
    # Espace disque (minimum 2GB)
    available_space=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_space" -lt 2 ]; then
        print_warning "Espace disque faible: ${available_space}GB disponible"
    else
        print_step "Espace disque: ${available_space}GB disponible"
    fi
    
    # Ports disponibles
    for port in 3000 8000 5433 80 443; do
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            print_warning "Port $port déjà utilisé"
        fi
    done
}

# Création de la structure du projet
create_project_structure() {
    print_header "Création de la structure du projet"
    
    # Dossiers principaux
    mkdir -p {data/{raw,processed},logs,grafana/{dashboards/json,datasources,config},sql/init,nginx/{ssl,conf.d},exports}
    
    # Permissions
    chmod -R 755 data logs grafana sql nginx exports
    
    print_step "Structure des dossiers créée"
}

# Configuration des fichiers
setup_configuration_files() {
    print_header "Configuration des fichiers"
    
    # Fichier .env pour la stack de visualisation
    cat > .env.viz << EOF
# Configuration PostgreSQL Analytics
POSTGRES_DB=covid_analytics
POSTGRES_USER=analytics
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

# Configuration Grafana
GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD}

# Configuration API
API_DATA_DIR=/app/data
DATABASE_URL=postgresql://analytics:${POSTGRES_PASSWORD}@postgres-analytics:5432/covid_analytics
REDIS_URL=redis://redis:6379/0

# Configuration réseau
COMPOSE_PROJECT_NAME=${PROJECT_NAME}
EOF

    # Configuration Grafana datasources
    cat > grafana/datasources/datasources.yml << 'EOF'
apiVersion: 1
datasources:
  - name: PostgreSQL-Analytics
    type: postgres
    uid: postgres-analytics
    access: proxy
    url: postgres-analytics:5432
    database: covid_analytics
    user: analytics
    secureJsonData:
      password: analytics123
    jsonData:
      sslmode: disable
      postgresVersion: 1500
    isDefault: true
  - name: COVID-API
    type: grafana-simple-json-datasource
    uid: covid-api
    access: proxy
    url: http://covid-api:8000/grafana
    isDefault: false
EOF

    # Configuration dashboard
    cat > grafana/dashboards/dashboard-config.yml << 'EOF'
apiVersion: 1
providers:
  - name: 'COVID Dashboards'
    orgId: 1
    folder: 'COVID Surveillance'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards/json
EOF

    print_step "Fichiers de configuration créés"
}

# Construction et démarrage des services
build_and_start_services() {
    print_header "Construction et démarrage des services"
    
    # Arrêt des services existants
    docker-compose -f docker-compose-viz.yml --env-file .env.viz down 2>/dev/null || true
    
    print_step "Arrêt des anciens services effectué"
    
    # Construction des images
    echo "🔨 Construction des images Docker..."
    docker-compose -f docker-compose-viz.yml --env-file .env.viz build --no-cache
    
    print_step "Images Docker construites"
    
    # Démarrage des services de base (PostgreSQL, Redis)
    echo "🚀 Démarrage des services de données..."
    docker-compose -f docker-compose-viz.yml --env-file .env.viz up -d postgres-analytics redis
    
    # Attendre que PostgreSQL soit prêt
    echo "⏳ Attente du démarrage de PostgreSQL..."
    for i in {1..30}; do
        if docker-compose -f docker-compose-viz.yml --env-file .env.viz exec postgres-analytics pg_isready -U analytics -d covid_analytics &>/dev/null; then
            break
        fi
        sleep 2
        echo -n "."
    done
    echo
    
    print_step "Base de données PostgreSQL prête"
    
    # Démarrage des services applicatifs
    echo "🚀 Démarrage des services applicatifs..."
    docker-compose -f docker-compose-viz.yml --env-file .env.viz up -d covid-api grafana
    
    print_step "Services applicatifs démarrés"
    
    # Démarrage du service ETL
    echo "🔄 Démarrage du service ETL..."
    docker-compose -f docker-compose-viz.yml --env-file .env.viz up -d etl-service
    
    print_step "Service ETL démarré"
}

# Test des services
test_services() {
    print_header "Test des services"
    
    # Test PostgreSQL
    echo "🔍 Test de PostgreSQL..."
    if docker-compose -f docker-compose-viz.yml --env-file .env.viz exec postgres-analytics psql -U analytics -d covid_analytics -c "SELECT version();" &>/dev/null; then
        print_step "PostgreSQL opérationnel"
    else
        print_error "PostgreSQL non accessible"
        return 1
    fi
    
    # Test API (avec retry)
    echo "🔍 Test de l'API FastAPI..."
    for i in {1..10}; do
        if curl -s http://localhost:8000/ | grep -q "healthy"; then
            print_step "API FastAPI opérationnelle"
            break
        fi
        if [ $i -eq 10 ]; then
            print_error "API FastAPI non accessible"
            return 1
        fi
        sleep 3
    done
    
    # Test Grafana (avec retry)
    echo "🔍 Test de Grafana..."
    for i in {1..15}; do
        if curl -s http://localhost:3000/api/health | grep -q "ok"; then
            print_step "Grafana opérationnel"
            break
        fi
        if [ $i -eq 15 ]; then
            print_error "Grafana non accessible"
            return 1
        fi
        sleep 3
    done
    
    # Test Redis
    echo "🔍 Test de Redis..."
    if docker-compose -f docker-compose-viz.yml --env-file .env.viz exec redis redis-cli ping | grep -q "PONG"; then
        print_step "Redis opérationnel"
    else
        print_warning "Redis non accessible (non critique)"
    fi
}

# Chargement des données de test
load_test_data() {
    print_header "Chargement des données de test"
    
    # Vérification de l'existence des données
    if [ ! -f "data/processed/latest_covid_processed.parquet" ]; then
        print_warning "Aucune donnée Parquet trouvée"
        print_warning "Exécutez d'abord le pipeline Airflow pour générer les données"
        print_warning "Ou créez des données de test avec: python generate_test_data.py"
        return 0
    fi
    
    # Exécution de l'ETL pour charger les données
    echo "🔄 Exécution de l'ETL pour charger les données..."
    if docker-compose -f docker-compose-viz.yml --env-file .env.viz exec etl-service python etl_postgres.py; then
        print_step "Données chargées en PostgreSQL"
    else
        print_warning "Échec du chargement ETL (continuez avec des données vides)"
    fi
}

# Configuration des dashboards Grafana
setup_grafana_dashboards() {
    print_header "Configuration des dashboards Grafana"
    
    # Import du dashboard principal (optionnel)
    if [ -f "grafana/dashboards/json/covid-overview.json" ]; then
        echo "📊 Import du dashboard COVID..."
        # Le dashboard sera automatiquement importé via le provisioning
        print_step "Dashboard configuré via provisioning"
    else
        print_warning "Dashboard JSON non trouvé, création manuelle requise"
    fi
    
    # Configuration des alertes Grafana (optionnel)
    echo "🔔 Configuration des alertes Grafana..."
    print_step "Alertes configurables via l'interface web"
}

# Affichage des informations de connexion
show_access_info() {
    print_header "Informations d'accès"
    
    echo -e "${BLUE}🌐 Services accessibles :${NC}"
    echo ""
    echo -e "  📊 ${GREEN}Grafana Dashboard${NC}"
    echo -e "     URL: http://localhost:3000"
    echo -e "     Identifiants: admin / ${GRAFANA_ADMIN_PASSWORD}"
    echo ""
    echo -e "  🔌 ${GREEN}API FastAPI${NC}"
    echo -e "     URL: http://localhost:8000"
    echo -e "     Documentation: http://localhost:8000/docs"
    echo -e "     Health: http://localhost:8000/"
    echo ""
    echo -e "  🗄️  ${GREEN}PostgreSQL Analytics${NC}"
    echo -e "     Host: localhost:5433"
    echo -e "     Database: covid_analytics"
    echo -e "     User: analytics / ${POSTGRES_PASSWORD}"
    echo ""
    echo -e "  🔴 ${GREEN}Redis Cache${NC}"
    echo -e "     Host: localhost:6379"
    echo ""
    
    echo -e "${YELLOW}📝 Commandes utiles :${NC}"
    echo ""
    echo "  # Voir les logs des services"
    echo "  docker-compose -f docker-compose-viz.yml logs -f [service_name]"
    echo ""
    echo "  # Redémarrer un service"
    echo "  docker-compose -f docker-compose-viz.yml restart [service_name]"
    echo ""
    echo "  # Arrêter tous les services"
    echo "  docker-compose -f docker-compose-viz.yml down"
    echo ""
    echo "  # Exécuter l'ETL manuellement"
    echo "  docker-compose -f docker-compose-viz.yml exec etl-service python etl_postgres.py"
    echo ""
    
    echo -e "${GREEN}✨ Stack de visualisation déployée avec succès !${NC}"
}

# Fonction de nettoyage en cas d'erreur
cleanup_on_error() {
    print_error "Erreur détectée, nettoyage en cours..."
    docker-compose -f docker-compose-viz.yml --env-file .env.viz down 2>/dev/null || true
    exit 1
}

# Trap pour nettoyage automatique
trap cleanup_on_error ERR

# Fonction principale
main() {
    print_header "🦠 Déploiement de la Stack de Visualisation COVID-19"
    
    check_prerequisites
    create_project_structure
    setup_configuration_files
    build_and_start_services
    
    # Petite pause pour que les services se stabilisent
    echo "⏳ Stabilisation des services (30s)..."
    sleep 30
    
    test_services
    load_test_data
    setup_grafana_dashboards
    show_access_info
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi