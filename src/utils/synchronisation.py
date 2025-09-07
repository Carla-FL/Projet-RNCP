# Imports pour la configuration
import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")  
# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils.load import Load
from datetime import datetime, timedelta
from prefect.schedules import Interval
from src.utils.extraction import Extraction
from src.Pipeline1.etl import main_etl
from prefect import flow, get_run_logger

# écraser l'ancienne base et la mettre à jours avec les nouvelles données

@flow(name='mise_a_jour', description="Pipeline de mise à jour des données YouTube", retries=1, retry_delay_seconds=60)
def maj():
    # etape 0 : se connecter à la base de données MongoDB
    logger = get_run_logger()
    logger.info("Début du processus de mise à jour des données YouTube")
    loader = Load()
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
                # logger.info(f"l url {video_url}, la collection {collection}, la base {db_name}")
                #last_comment_id = db_collection.find_one(sort=[("publishedAt", -1)], projection={"id":1,"_id": 0})  # dernier commentaire
                #logger.info(f"le dernier commentaire qui foire tout {last_comment_id}")
                
                list_urls.append(video_url['url'])
                
                #id_list = db_collection.distinct("id")
                #id_lists.append(id_list)
                # last_comment_ids.append(last_comment_id['id'])
            else:
                pass # ajouter un truc pour ajouter les info s'il n'y a pas d'url ajouter un paramètre a main_etl
    loader.data_base_deconnexion(client)
    for url in list_urls:
        
        logger.info(f"main_etl s'execute avec les paramètres :{url}" )
        main_etl(url, maj=True)  # Appel de la fonction main_etl pour chaque URL

if __name__ == "__main__":
    maj.serve(name="mise_a_jour", schedule=Interval( timedelta(minutes=60), anchor_date=datetime(2025, 8, 23, 15, 0), timezone="Europe/Paris")) #cron="0 10 * * MON")  # Lancer la mise à jour    
