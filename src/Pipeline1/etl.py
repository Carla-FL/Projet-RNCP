import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")  # Ajoute le répertoire parent au chemin de recherche des modules
from src.utils.extraction import Extraction
from src.utils.transformation import main_transformation
from src.utils.load import Load
from prefect import flow, task
from prefect.logging import get_run_logger



@flow(name='main_etl_flow', description="Pipeline ETL principal pour l'analyse des vidéos YouTube")
def main_etl(url:str,last_id=None, with_channel_id:bool=False, maj=False):
    """
    
    """
    logger = get_run_logger()
    try:
        
    
        #logger.info("Début du pipeline ETL pour l'URL : %s", url)
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
        #raise ValueError(f"Erreur dans le pipeline ETL : {e}")

""" ________________________________________________________________  Main  ________________________________________________________________"""
# if __name__ == "__main__":
#     main_etl.serve(name="main_etl_flow", cron="0 10 * * MON")  # Exemple d'URL YouTube

