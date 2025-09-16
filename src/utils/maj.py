import sys
import os
import pathlib
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
# from src.Pipeline1.etl import main_etl
import pandas as pd


# Configuration du chemin - compatible local et GitHub Actions
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # Remonte au root du projet
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.path.insert(0, os.path.join(project_root, 'src', 'utils'))

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/sync.log', 'a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class YouTubeSynchronizer:
    """Gestionnaire de synchronisation YouTube pour GitHub Actions"""
    
    def __init__(self):
        self.force_update = os.getenv('FORCE_UPDATE', 'false').lower() == 'true'
        self.summary = {
            'start_time': datetime.now().isoformat(),
            'records_processed': 0,
            'videos_updated': 0,
            'errors': []
        }
        
    def detect_environment(self):
        """Détecte l'environnement d'exécution"""
        if os.getenv('GITHUB_ACTIONS'):
            logger.info("Environnement: GitHub Actions")
            return 'github_actions'
        else:
            logger.info("Environnement: Local")
            return 'local'
    
    def setup_imports(self):
        """Configure les imports selon l'environnement"""
        try:
            # Essayer différents chemins d'import
            import_success = False
            
            # Tentative 1: Import direct
            try:
                from load import Load
                from etl import main_etl
                import_success = True
                logger.info("Imports directs réussis")
            except ImportError:
                pass
            
            # Tentative 2: Import depuis src
            if not import_success:
                try:
                    from src.utils.load import Load
                    from src.Pipeline1.etl import main_etl
                    import_success = True
                    logger.info("Imports depuis src réussis")
                except ImportError:
                    pass
            
            # Tentative 3: Import avec chemins explicites
            if not import_success:
                try:
                    sys.path.append(os.path.join(project_root, 'src', 'utils'))
                    sys.path.append(os.path.join(project_root, 'src', 'Pipeline1'))
                    from load import Load
                    from etl import main_etl
                    import_success = True
                    logger.info("Imports avec chemins explicites réussis")
                except ImportError as e:
                    logger.error(f"Toutes les tentatives d'import ont échoué: {e}")
                    return False
            
            if import_success:
                self.Load = Load
                self.main_etl = main_etl
                return True
            
        except Exception as e:
            logger.error(f"Erreur configuration imports: {e}")
            self.summary['errors'].append(f"Import error: {str(e)}")
            return False
    
    def get_existing_videos(self):
        """Récupère les URLs des vidéos existantes dans la base"""
        try:
            loader = self.Load()
            client = loader.data_base_connexion()
            
            list_urls = []
            
            # Adapter selon l'architecture (cloud vs local)
            try:
                # Essayer d'importer streamlit_config (architecture cloud)
                from src.app.streamlit_config import get_database_connections
                db_config = get_database_connections()
                target_db = db_config["mongodb"]["database"]
                
                # Mode cloud : une seule base
                db = client[target_db]
                for collection_name in db.list_collection_names():
                    collection = db[collection_name]
                    video_url_doc = collection.find_one({}, {"url": 1, "_id": 0})
                    if video_url_doc and "url" in video_url_doc:
                        list_urls.append(video_url_doc["url"])
                        
            except ImportError:
                # Mode local : plusieurs bases
                for db_name in client.list_database_names():
                    if db_name in ("admin", "config", "local", "test"):
                        continue
                    
                    db = client[db_name]
                    for collection_name in db.list_collection_names():
                        collection = db[collection_name]
                        video_url_doc = collection.find_one({}, {"url": 1, "_id": 0})
                        if video_url_doc and "url" in video_url_doc:
                            list_urls.append(video_url_doc["url"])
            
            loader.data_base_deconnexion(client)
            logger.info(f"URLs trouvées: {len(list_urls)}")
            return list_urls
            
        except Exception as e:
            logger.error(f"Erreur récupération URLs: {e}")
            self.summary['errors'].append(f"URL retrieval: {str(e)}")
            return []
    
    def process_video_url(self, url):
        """Traite une URL de vidéo"""
        try:
            logger.info(f"Traitement: {url}")
            
            # Utiliser main_etl avec maj=True
            result = self.main_etl(url, maj=True)
            
            if result is not None:
                self.summary['videos_updated'] += 1
                logger.info(f"URL {url} traitée avec succès")
                return True
            else:
                logger.warning(f"URL {url} - résultat None")
                return False
                
        except Exception as e:
            logger.error(f"Erreur traitement {url}: {e}")
            self.summary['errors'].append(f"URL {url}: {str(e)}")
            return False
    
    def run_synchronization(self):
        """Exécute la synchronisation complète"""
        logger.info("Début synchronisation YouTube")
        
        # Détecter l'environnement
        env = self.detect_environment()
        
        # Configurer les imports
        if not self.setup_imports():
            logger.error("Impossible de configurer les imports")
            return False
        
        # Récupérer les URLs existantes
        urls_to_process = self.get_existing_videos()
        
        if not urls_to_process:
            logger.warning("Aucune URL trouvée à traiter")
            return True
        
        # Traiter chaque URL
        success = True
        processed_count = 0
        
        for url in urls_to_process:
            try:
                if self.process_video_url(url):
                    processed_count += 1
                else:
                    success = False
                
                # Pause entre vidéos
                import time
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Erreur critique pour {url}: {e}")
                success = False
        
        # Finaliser
        self.summary['end_time'] = datetime.now().isoformat()
        self.summary['duration_minutes'] = (
            datetime.fromisoformat(self.summary['end_time']) - 
            datetime.fromisoformat(self.summary['start_time'])
        ).total_seconds() / 60
        self.summary['processed_count'] = processed_count
        
        self.save_summary()
        
        if success:
            logger.info(f"Synchronisation terminée: {processed_count} vidéos traitées")
        else:
            logger.warning(f"Synchronisation avec erreurs: {processed_count} vidéos traitées")
        
        return success
    
    def save_summary(self):
        """Sauvegarde le résumé"""
        try:
            with open('logs/summary.json', 'w', encoding='utf-8') as f:
                json.dump(self.summary, f, indent=2, ensure_ascii=False)
            
            logger.info("Résumé de l'exécution:")
            logger.info(f"  - Durée: {self.summary.get('duration_minutes', 0):.1f} minutes")
            logger.info(f"  - Vidéos traitées: {self.summary.get('processed_count', 0)}")
            logger.info(f"  - Erreurs: {len(self.summary['errors'])}")
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")

def main():
    """Point d'entrée principal"""
    try:
        synchronizer = YouTubeSynchronizer()
        success = synchronizer.run_synchronization()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Synchronisation interrompue")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()