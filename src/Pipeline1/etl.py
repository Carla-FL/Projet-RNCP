import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")  # Ajoute le répertoire parent au chemin de recherche des modules
from src.utils.extraction import Extraction
from src.utils.transformation import main_transformation
from src.utils.load import Load
from prefect import flow, task
from prefect.logging import get_run_logger

def channel_id_exists(input_video_url):
    """
    Utiliser le extract+loader pour verifier si le channel id exist
    """
    pass

@flow(name='main_etl_flow', description="Pipeline ETL principal pour l'analyse des vidéos YouTube")
def main_etl(url:str,last_id=None, with_channel_id:bool=False, maj=False):
    """
    Fonction principale pour exécuter le pipeline ETL.

    1 : on regarde pour cet url il existe déjà des données
    2 : si oui -> on regarde si on doit aujouter des colonnes 
    3 : si oui -> on reecharge toutes les données
    4 : si non -> on récupère les id existants et on les exclus du chargement 
    5 : si non -> on dcharge toutes les données
    
    Args:
        video_url (str): URL de la vidéo YouTube à analyser.
    
    Returns:
        None
    """
    logger = get_run_logger()
    try:
        
        # etape 0
        # se connecter au serveur de donnees et verifier si l'id de la vidéo existe deja
        # si oui -> on récupère la date de la dernière extraction et de la dernière publication
        # si non -> on lance le processus etl
        #videoid = Extraction(url).url2id()
        #logger.info("connection à la bsae")
        # Vérifier si le channel_id existe déjà
        # client = Load.data_base_connexion()

        # # Lister toutes les bases (hors bases système si besoin)
        # for db_name in client.list_database_names():
        #     if db_name in ("admin", "config", "local"):  # filtrer les bases internes MongoDB
        #         continue
            
        #     db = client[db_name]
        #     # Lister toutes les collections de cette base
        #     collections = db.list_collection_names()
        #     if videoid in collections:
        #         print(f"Le channel_id '{videoid}' existe déjà dans la base de données '{db_name}'. Voici les analyses disponibles")
        #         # on récupère le commentaire le plus récent
        #         last_id = db[videoid].find_one(sort=[("publishedAt", -1)])  # tri par date de publication décroissante
        #         # on affiche les données 

            #else:
        logger.info("Début du pipeline ETL pour l'URL : %s", url)
        # last_id = None
        input_video_url = url
        # Étape 1 : Extraction des données
        extraction = Extraction(video_url=input_video_url)
        data, video_id, channel_id = extraction.main_extraction()
        logger.info(f"fin de l'extraction des {data.shape[0]} données")
        # Étape 2 : Transformation des données
        transformed_data = main_transformation(data)
        logger.info("Transformation des données terminée.")

        # Étape 3 : Chargement des données
        loader = Load(maj)
        loader.load(transformed_data, video_id, channel_id)
        logger.info("Chargement des données terminé.")
        logger.info("Pipeline ETL terminé avec succès.")
        if with_channel_id:
            return channel_id
    except Exception as e:
        logger.info(f"Une erreur s'est produite dans le pipeline ETL : {e}")

""" ________________________________________________________________  Main  ________________________________________________________________"""
# if __name__ == "__main__":
#     main_etl.serve(name="main_etl_flow", cron="0 10 * * MON")  # Exemple d'URL YouTube

