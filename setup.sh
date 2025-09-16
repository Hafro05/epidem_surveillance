#!/bin/bash

# Script de setup pour le pipeline de surveillance épidémiologique
# Usage: ./setup.sh

set -e

echo "🚀 Configuration du pipeline de surveillance épidémiologique"
echo "============================================================"

# Configuration des variables
PROJECT_NAME="covid-surveillance-pipeline"
AIRFLOW_UID=$(id -u)

# Couleurs pour les messages
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Fonction pour vérifier les prérequis
check_prerequisites() {
    echo "📋 Vérification des prérequis..."
    
    # Vérifier Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker n'est pas installé. Installez Docker d'abord."
        exit 1
    fi
    print_status "Docker trouvé"
    
    # Vérifier Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose n'est pas installé."
        exit 1
    fi
    print_status "Docker Compose trouvé"
    
    # Vérifier Python
    if ! command -v python3 &> /dev/null; then
        print_warning "Python 3 n'est pas trouvé. Certains scripts pourraient ne pas fonctionner."
    else
        print_status "Python 3 trouvé"
    fi
}

# Création de la structure des dossiers
create_project_structure() {
    echo ""
    echo "📁 Création de la structure du projet..."
    
    # Dossiers principaux
    mkdir -p {dags,plugins,logs,data/{raw,processed},scripts,config}
    
    # Fichiers de configuration
    cat > .env << EOF
# Configuration Airflow
AIRFLOW_UID=${AIRFLOW_UID}
AIRFLOW_PROJ_DIR=.

# Configuration de l'utilisateur web Airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin123

# Configuration email (optionnel - remplacez par vos valeurs)
# AIRFLOW__SMTP__SMTP_USER=your-email@gmail.com
# AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
EOF

    # Création du requirements.txt
    cat > requirements.txt << EOF
# Dépendances Python pour le projet
pandas>=1.5.0
requests>=2.28.0
pyarrow>=10.0.0
numpy>=1.21.0
apache-airflow==2.7.1

# Optionnel pour les visualisations
matplotlib>=3.5.0
seaborn>=0.11.0

# Optionnel pour les notifications
slack-sdk>=3.0.0
EOF

    # Création du gitignore
    cat > .gitignore << EOF
# Airflow
logs/
airflow.cfg
airflow.db
webserver_config.py

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv/
.env

# Data
data/raw/*.csv
data/processed/*.parquet
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
EOF

    print_status "Structure du projet créée"
}

# Installation des dépendances Python locales
setup_python_environment() {
    echo ""
    echo "🐍 Configuration de l'environnement Python..."
    
    if command -v python3 &> /dev/null; then
        # Création d'un environnement virtuel
        python3 -m venv venv
        venv/Scripts/activate
        
        # Installation des dépendances
        pip install --upgrade pip
        pip install -r requirements.txt
        
        print_status "Environnement Python configuré (venv activé)"
        print_warning "Pour activer l'environnement: source venv/bin/activate"
    else
        print_warning "Python 3 n trouvé, environnement virtuel non créé"
    fi
}

# Copie des scripts dans les bons dossiers
setup_scripts() {
    echo ""
    echo "📋 Configuration des scripts..."
    
    # Copie des scripts (à adapter selon vos fichiers)
    echo "# Placez ici vos scripts d'ingestion et de transformation" > scripts/README.md
    
    # Configuration exemple pour Airflow
    mkdir -p dags/utils
    
    cat > dags/utils/__init__.py << 'EOF'
"""
Modules utilitaires pour les DAGs Airflow
"""
EOF

    cat > dags/utils/config.py << 'EOF'
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
EOF

    print_status "Scripts configurés"
}

# Initialisation d'Airflow
init_airflow() {
    echo ""
    echo "🎈 Initialisation d'Airflow..."
    
    # Démarrage des services
    docker-compose up airflow-init
    
    print_status "Airflow initialisé"
}

# Affichage des instructions finales
show_final_instructions() {
    echo ""
    echo "🎉 Configuration terminée !"
    echo "=========================="
    echo ""
    echo "Pour démarrer le projet :"
    echo "1. Démarrer Airflow : docker-compose up -d"
    echo "2. Accéder à l'interface : http://localhost:8080"
    echo "   - Username: admin"
    echo "   - Password: admin123"
    echo ""
    echo "3. Copier vos scripts Python dans le dossier 'dags/'"
    echo "4. Les données seront stockées dans 'data/'"
    echo ""
    echo "Commandes utiles :"
    echo "- Arrêter : docker-compose down"
    echo "- Voir les logs : docker-compose logs -f"
    echo "- Redémarrer : docker-compose restart"
    echo ""
    echo "Structure du projet créée :"
    echo "├── dags/          # DAGs Airflow"
    echo "├── plugins/       # Plugins Airflow"
    echo "├── logs/          # Logs Airflow"
    echo "├── data/          # Données (raw/processed)"
    echo "├── scripts/       # Scripts Python"
    echo "└── config/        # Configuration"
    echo ""
    print_status "Projet prêt ! 🚀"
}

# Fonction principale
main() {
    check_prerequisites
    create_project_structure
    setup_python_environment
    setup_scripts
    init_airflow
    show_final_instructions
}

# Exécution
main