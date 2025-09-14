# streamlit_config.py
"""
Configuration et utilitaires pour Streamlit Community Cloud
À importer dans ton app.py principal
"""

import streamlit as st
import os
import pandas as pd
from typing import Optional
import logging

# Configuration des secrets Streamlit
def get_secret(key: str, default: Optional[str] = None) -> str:
    """
    Récupère les secrets depuis Streamlit Cloud ou variables d'environnement
    """
    # D'abord essayer les secrets Streamlit
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    
    # Fallback vers les variables d'environnement
    return os.getenv(key, default)

# Configuration des bases de données cloud
def get_database_connections():
    """
    Configuration des connexions aux bases de données cloud
    """
    return {
        # MongoDB Atlas
        "mongodb": {
            "connection_string": get_secret("MONGODB_CONNECTION_STRING"),
            "database": get_secret("MONGODB_DATABASE", "youtube-analysis"),
            "username": get_secret("MONGO_USERNAME"),
            "password": get_secret("MONGO_PASSWORD"),
        },
        
        # Neon PostgreSQL
        "postgresql": {
            "connection_string": get_secret("POSTGRESQL_CONNECTION_STRING"),
            "host": get_secret("POSTGRES_HOST"),
            "database": get_secret("POSTGRES_DB", "prefect"),
            "username": get_secret("POSTGRES_USER", "prefect"),
            "password": get_secret("POSTGRES_PASSWORD"),
            "port": get_secret("POSTGRES_PORT", "5432"),
        }
    }

# Configuration des modèles
def get_model_config():
    """
    Configuration des modèles Hugging Face
    """
    return {
        "hf_token": get_secret("HF_TOKEN"),
        "model_priority": get_secret("MODEL_PRIORITY", "finetuned,original,logistic").split(","),
        "cache_dir": "./.model_cache"  # Streamlit Cloud supporte le cache local
    }

# Configuration des API
def get_api_config():
    """
    Configuration des clés API
    """
    return {
        "youtube_api_key": get_secret("DEVELOPER_KEY"),
        "youtube_quota_per_day": 10000  # Limite par défaut
    }

# Optimisations pour Streamlit Cloud
@st.cache_data
def load_cached_data(cache_key: str):
    """
    Cache optimisé pour Streamlit Cloud
    """
    pass

@st.cache_resource 
def get_model_manager():
    """
    Cache le gestionnaire de modèles pour éviter les rechargements
    """
    from model_manager import SentimentModelManager
    manager = SentimentModelManager()
    manager.load_best_available_model()
    return manager

# Configuration de logging pour Streamlit Cloud
def setup_logging():
    """
    Configuration du logging optimisée pour Streamlit Cloud
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Streamlit Cloud capture stdout
        ]
    )
    
    return logging.getLogger(__name__)

# Fonction pour tester les connexions
def test_connections():
    """
    Test des connexions aux services externes
    Utile pour debugging sur Streamlit Cloud
    """
    results = {}
    
    # Test MongoDB Atlas
    try:
        import pymongo
        db_config = get_database_connections()["mongodb"]
        if db_config["connection_string"]:
            client = pymongo.MongoClient(db_config["connection_string"])
            client.admin.command('ping')
            results["mongodb"] = "✅ Connecté"
            client.close()
        else:
            results["mongodb"] = "❌ Connection string manquante"
    except Exception as e:
        results["mongodb"] = f"❌ Erreur: {str(e)}"
    
    # Test PostgreSQL Neon
    try:
        import psycopg2
        db_config = get_database_connections()["postgresql"]
        if db_config["connection_string"]:
            conn = psycopg2.connect(db_config["connection_string"])
            conn.close()
            results["postgresql"] = "✅ Connecté"
        else:
            results["postgresql"] = "❌ Connection string manquante"
    except Exception as e:
        results["postgresql"] = f"❌ Erreur: {str(e)}"
    
    # Test Hugging Face
    try:
        from huggingface_hub import HfApi
        hf_token = get_secret("HF_TOKEN")
        if hf_token:
            api = HfApi(token=hf_token)
            # Test simple
            results["huggingface"] = "✅ Token valide"
        else:
            results["huggingface"] = "❌ Token manquant"
    except Exception as e:
        results["huggingface"] = f"❌ Erreur: {str(e)}"
    
    return results

# Fonction principale pour initialiser l'app
def initialize_streamlit_app():
    """
    Initialisation complète de l'app Streamlit
    À appeler au début de ton app.py
    """
    # Configuration de la page
    st.set_page_config(
        page_title="YouTube Sentiment Analysis",
        page_icon="🎥",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Setup logging
    logger = setup_logging()
    
    # Vérifier les configurations critiques
    required_secrets = ["MONGODB_CONNECTION_STRING", "DEVELOPER_KEY", "HF_TOKEN"]
    missing_secrets = []
    
    for secret in required_secrets:
        if not get_secret(secret):
            missing_secrets.append(secret)
    
    if missing_secrets:
        st.error(f"⚠️ Secrets manquants: {', '.join(missing_secrets)}")
        st.info("Configure les secrets dans Streamlit Cloud: Settings > Secrets")
        st.stop()
    
    return logger

# Helper pour la sidebar de debug
def add_debug_sidebar():
    """
    Ajoute une sidebar de debug pour Streamlit Cloud
    """
    with st.sidebar.expander("🔧 Debug Info", expanded=False):
        if st.button("Test Connections"):
            with st.spinner("Test des connexions..."):
                results = test_connections()
                for service, status in results.items():
                    st.write(f"**{service}**: {status}")
        
        # Info sur les modèles
        try:
            model_manager = get_model_manager()
            info = model_manager.get_model_info()
            st.write("**Modèle actuel:**")
            st.json(info)
        except Exception as e:
            st.write(f"**Modèle**: ❌ Erreur - {str(e)}")

        # Info sur l'environnement
        st.write("**Environnement:**")
        env_info = {
            "Streamlit version": st.__version__,
            "Mode cloud": bool(get_secret("STREAMLIT_SERVER_HEADLESS")),
            "Cache dir": "./.model_cache"
        }
        st.json(env_info)

# Configuration spécifique pour éviter le sleep de Streamlit Cloud
def keep_alive_setup():
    """
    Configuration pour éviter que l'app s'endorme
    """
    # Ajouter un ping automatique en arrière-plan si nécessaire
    app_url = get_secret("STREAMLIT_APP_URL")
    if app_url:
        st.sidebar.info(f"App URL: {app_url}")

# Fonction utilitaire pour gérer les erreurs
def handle_streamlit_error(error: Exception, context: str = ""):
    """
    Gestion centralisée des erreurs pour Streamlit
    """
    error_msg = f"Erreur {context}: {str(error)}"
    st.error(error_msg)
    
    # Log pour debugging
    logger = setup_logging()
    logger.error(error_msg)
    
    # Informations de debug en expander
    with st.expander("🐛 Détails de l'erreur", expanded=False):
        st.code(str(error))
        st.write(f"Type: {type(error).__name__}")

# Helper pour l'affichage des DataFrames
def display_dataframe_info(df: pd.DataFrame, title: str = "DataFrame Info"):
    """
    Affiche les informations sur un DataFrame de manière propre
    """
    with st.expander(f"📊 {title}", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Lignes", len(df))
        with col2:
            st.metric("Colonnes", len(df.columns))
        with col3:
            st.metric("Mémoire (MB)", round(df.memory_usage(deep=True).sum() / 1024**2, 2))
        
        st.write("**Colonnes:**", list(df.columns))
        if len(df) > 0:
            st.dataframe(df.head())