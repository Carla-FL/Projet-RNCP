import sys
import os
import pathlib
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.Pipeline1.etl import main_etl
import pandas as pd

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging pour GitHub Actions
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
        self.target_channel = os.getenv('TARGET_CHANNEL', '')
        self.summary = {
            'start_time': datetime.now().isoformat(),
            'records_processed': 0,
            'videos_updated': 0,
            'errors': [],
            'channels_processed': []
        }
        
    def detect_environment(self):
        """Détecte si on est dans GitHub Actions ou en local"""
        if os.getenv('GITHUB_ACTIONS'):
            logger.info("Environnement détecté: GitHub Actions")
            return 'github_actions'
        elif os.getenv('STREAMLIT_SERVER_HEADLESS'):
            logger.info("Environnement détecté: Streamlit Cloud")
            return 'streamlit_cloud'
        else:
            logger.info("Environnement détecté: Local")
            return 'local'
    
    def setup_imports(self):
        """Configure les imports selon l'environnement"""
        try:
            # Imports spécifiques à ton projet
            from load import Load
            from transformation import main_transformation

            
            self.Load = Load
            self.main_transformation = main_transformation
            logger.info("Modules importés avec succès")
            return True
            
        except ImportError as e:
            logger.error(f"Erreur d'import: {e}")
            self.summary['errors'].append(f"Import error: {str(e)}")
            return False
    
    def get_channels_to_process(self):
        """Détermine quels channels traiter"""
        if self.target_channel:
            logger.info(f"Traitement du channel spécifique: {self.target_channel}")
            return [self.target_channel]
        
        # Liste de tes channels configurés

        channels = []
        
        # Option 1: Depuis variables d'environnement
        channels_env = os.getenv('YOUTUBE_CHANNELS', '')
        if channels_env:
            channels = channels_env.split(',')
        
        # Option 2: Liste hardcodée (à adapter)
        if not channels:
            channels = [
                'UCDkl5M0WVaddTWE4rr2cSeA',
            ]
        
        logger.info(f"Channels à traiter: {len(channels)} - {channels}")
        return channels
    
    def get_videos_for_channel(self, channel_id):
        """Récupère les vidéos à traiter pour un channel"""
        try:
            # Utiliser ton système existant pour récupérer les vidéos
            # À adapter selon ton code d'extraction
            
            # Exemple simplifié - remplace par ton code réel
            loader = self.Load()
            
            client = loader.data_base_connexion()

            # etape 2 : récupérer les urls des vidéos de la base de données
            list_urls = []
            # id_lists = []  # pour stocker les derniers ids de commentaires

            for db_name in client.list_database_names():
                # logger.info(f"traitement base {db_name}")
                if db_name in ("admin", "config", "local", "test"):  # filtrer les bases internes MongoDB
                    # logger.info(f"bases trouvées {db_name}")
                    continue
                db = client[db_name]
                # Lister toutes les collections de cette base
                for collection in db.list_collection_names():
                    # logger.info(f"traitement de la collection {collection}")
                    db_collection = db[collection]
                    # Récupérer l'URL de la vidéo à partir de la collection
                    video_url = db_collection.find_one({}, {"url": 1, "_id": 0})
                    logger.info(f"taille de video_url {len(video_url)}")
                    if len(video_url)>0:
                        list_urls.append(video_url['url'])
                    else:
                        pass # ajouter un truc pour ajouter les info s'il n'y a pas d'url ajouter un paramètre a main_etl
            loader.data_base_deconnexion(client)
            
            logger.info(f"Channel {channel_id}: {len(list_urls)} vidéos à traiter")
            return list_urls
            
        except Exception as e:
            logger.error(f"Erreur récupération vidéos pour {channel_id}: {e}")
            self.summary['errors'].append(f"Channel {channel_id}: {str(e)}")
            return []
    
    def process_video(self, channel_id, video_url):
        """Traite une vidéo spécifique"""
        try:
            logger.info(f"Traitement de : {video_url} vidéos. (Channel: {channel_id})")
            
            # Étape 1: Extraction des données
            # À remplacer par ton code d'extraction réel
            # df_extracted = self.main_extraction(video_id, channel_id)
            
            # Pour l'instant, simulation
            df_extracted = video_url
            
            if df_extracted is None or df_extracted.empty:
                logger.warning(f"Aucune donnée extraite pour {video_url.split("v=")[-1][:11]}")
                return False
            
            # Étape 2: Transformation des données
            df_transformed = main_etl(video_url, maj=True)
            
            if df_transformed.empty:
                logger.warning(f"Aucune donnée après transformation pour {video_url.split("v=")[-1][:11]}")
                return False
            
            # # Étape 3: Chargement en base
            # loader = self.Load(maj=True)  # Mode mise à jour
            # loader.load(df_transformed, video_id, channel_id)
            
            # Mise à jour des statistiques
            self.summary['records_processed'] += len(df_transformed)
            self.summary['videos_updated'] += 1
            
            logger.info(f"Vidéo {video_url} traitée avec succès: {len(df_transformed)} commentaires")
            return True
            
        except Exception as e:
            logger.error(f"Erreur traitement vidéo {video_url}: {e}")
            self.summary['errors'].append(f"Video {video_url}: {str(e)}")
            return False
    
    def process_channel(self, channel_id):
        """Traite un channel complet"""
        try:
            logger.info(f"Début traitement channel: {channel_id}")
            
            # Récupérer les vidéos à traiter
            videos = self.get_videos_for_channel(channel_id)
            
            if not videos:
                logger.info(f"Aucune vidéo à traiter pour le channel {channel_id}")
                return True
            
            # Traiter chaque vidéo
            success_count = 0
            for video_url in videos:
                if self.process_video(channel_id, video_url):
                    success_count += 1
                
                # Pause pour éviter les limites de rate
                import time
                time.sleep(1)
            
            self.summary['channels_processed'].append({
                'channel_id': channel_id,
                'videos_processed': success_count,
                'total_videos': len(videos)
            })
            
            logger.info(f"Channel {channel_id} terminé: {success_count}/{len(videos)} vidéos traitées")
            return True
            
        except Exception as e:
            logger.error(f"Erreur traitement channel {channel_id}: {e}")
            self.summary['errors'].append(f"Channel {channel_id}: {str(e)}")
            return False
    
    def run_synchronization(self):
        """Exécute la synchronisation complète"""
        logger.info("🚀 Début de la synchronisation YouTube")
        
        # Vérifier l'environnement
        env = self.detect_environment()
        
        # Configurer les imports
        if not self.setup_imports():
            logger.error("Impossible de configurer les imports")
            return False
        
        # Récupérer les channels à traiter
        channels = self.get_channels_to_process()
        
        if not channels:
            logger.error("Aucun channel à traiter")
            return False
        
        # Traiter chaque channel
        success = True
        for channel_id in channels:
            if not self.process_channel(channel_id):
                success = False
        
        # Finaliser
        self.summary['end_time'] = datetime.now().isoformat()
        self.summary['duration_minutes'] = (
            datetime.fromisoformat(self.summary['end_time']) - 
            datetime.fromisoformat(self.summary['start_time'])
        ).total_seconds() / 60
        
        self.save_summary()
        
        if success:
            logger.info("✅ Synchronisation terminée avec succès")
        else:
            logger.warning("⚠️ Synchronisation terminée avec des erreurs")
        
        return success
    
    def save_summary(self):
        """Sauvegarde le résumé de l'exécution"""
        try:
            os.makedirs('logs', exist_ok=True)
            
            # Sauvegarder le summary en JSON
            with open('logs/summary.json', 'w') as f:
                json.dump(self.summary, f, indent=2, ensure_ascii=False)
            
            # Log du résumé
            logger.info("📊 Résumé de l'exécution:")
            logger.info(f"  - Durée: {self.summary.get('duration_minutes', 0):.1f} minutes")
            logger.info(f"  - Records traités: {self.summary['records_processed']}")
            logger.info(f"  - Vidéos mises à jour: {self.summary['videos_updated']}")
            logger.info(f"  - Erreurs: {len(self.summary['errors'])}")
            
            if self.summary['errors']:
                logger.warning("Erreurs rencontrées:")
                for error in self.summary['errors'][:5]:  # Limiter à 5 pour les logs
                    logger.warning(f"  - {error}")
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde summary: {e}")

def main():
    """Point d'entrée principal"""
    try:
        # Créer le dossier logs si nécessaire
        os.makedirs('logs', exist_ok=True)
        
        # Initialiser et lancer la synchronisation
        synchronizer = YouTubeSynchronizer()
        success = synchronizer.run_synchronization()
        
        # Code de sortie pour GitHub Actions
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Synchronisation interrompue par l'utilisateur")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()