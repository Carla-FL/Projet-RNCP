""" ________________________________________________________________   Library   ________________________________________________________________"""
import re
import os
import time
import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task
from googleapiclient.discovery import build
from langdetect import detect
from .load import Load
from prefect.logging import get_run_logger
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
load_dotenv()
""" ________________________________________________________________  Fonctions  ________________________________________________________________"""
class Extraction :
    def __init__(self, video_url:str =""):
        self.api_key = os.getenv('DEVELOPER_KEY')
        self.video_url = video_url
        #self.last_comment = last_comment  # id du commentaire le plus récent
        self.video_id = self.url2id()
        self.channel_id = None
        self.existing_comments_id = None
    
    
    # récupérer l'id de la vidéo à partir de l'URL
    def url2id(self):
        pattern = r'^(https?:\/\/)?(www\.)?(youtube\.com\/(watch\?v=|embed\/|v\/)|youtu\.be\/)([\w-]{11})(\S+)?$'
        if re.match(pattern, self.video_url) :
            print("Valid YouTube URL")
        else:
            raise ValueError('Error : invalid YouTube URL')

        video_id = self.video_url.split("v=")[-1][:11]
        return video_id

    # récupère le secret id pour l'appel d'API : faire os.getenv('DEVELOPER_KEY') ou lire dans un fichier
    # def get_key(self):
    #     with open('secret_clientid.txt', 'r') as file:
    #         DEVELOPER_KEY = file.read()
    #     return DEVELOPER_KEY

    @task(name='get_data_task', description="Tâche d'extraction des données YouTube")
    # appel de l'API
    def get_data(self):
        #logger = get_run_logger()
        api_key = self.api_key # .get_key() #
        youtube = build("youtube", "v3", developerKey=api_key)

        # Paramètres initiaux pour la requête
        video_id = self.url2id()
        comments_data = []
        next_page_token = None

        # Récupérer les infos de la vidéo
        try:
            video_response = youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            ).execute()

            video_info = video_response["items"][0]["snippet"]
            self.channel_id = video_info["channelId"]
            self.exisitng_comments_id =  Load().check_exisitng_data(self.channel_id, self.video_id)
            
        except Exception as e:
            raise RuntimeError(f"Impossible de récupérer les infos de la vidéo : {e}")
        

        # Vérifier la langue de la chaîne
        try:

            video_title = video_info["title"]
            video_description = video_info["description"]
            lang = detect(video_title + " " + video_description)
            print(f"Langue de la chaîne : {lang}")

            if lang != "fr":
                raise ValueError(f"Langue détectée : {lang.upper()}, ce script ne traite que les chaînes françaises.")
        except Exception as e:
            raise RuntimeError(f"Impossible de vérifier la langue de la chaîne : {e}")
        
        # Vérifier le nombre de commentaires
        try:
            video_nb_comments = int(video_response["items"][0]["statistics"]["commentCount"])
            if video_nb_comments<200:
                raise ValueError('Error : not enough comments')
        except Exception as e:
            raise RuntimeError(f"Impossible de récupérer le nombre de commentaires : {e}")
        

        # Étape 3 – Collecte des commentaires
        print("Langue valide \n"
        "Nombre de commentaires suffisant \n"
        "Récupération des commentaires en cours...")

        while True:
            response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            order="time",
            textFormat="plainText",
            pageToken=next_page_token
            ).execute()
            
            # Ajouter les commentaires récupérés à la liste
            extraction_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            for item in response.get("items", []):
                # logger.info(f"on compare : {item['id']} et {self.last_comment}")
                comment_info = item["snippet"]["topLevelComment"]["snippet"]
                # if item["id"] not in self.exisitng_comments_id:
                # Vérifier si le commentaire a déjà été extrait :
                if comment_info.get("channelId") == comment_info.get("authorChannelId"):
                    continue
                
                comments_data.append({
                    "url": self.video_url,
                    "id": item["id"],
                    "titre" : video_title,
                    "channelId": comment_info.get("channelId"),
                    "videoId": comment_info.get("videoId"),	
                    "author": comment_info.get("authorDisplayName"),
                    "publishedAt": comment_info.get("publishedAt"),
                    "comment": comment_info.get("textOriginal"),
                    "likeCount": comment_info.get("likeCount"),
                    "extractedAt": extraction_date
                })

            # Vérifier s'il y a une page suivante
            time.sleep(5)
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(1)

        print(f'data uploaded')
        return comments_data

    # Fonction pour créer un DataFrame à partir des données collectées
    def get_data_table(self)-> pd.DataFrame:
        # Création du DataFrame à partir des données collectées
        df = pd.DataFrame(self.get_data())
        # print(df.head())
        print(f"Total de commentaires récupérés : {df.shape[0]}")
        return df
    
    @flow(name='extraction_pipeline', description="Pipeline d'extraction des données YouTube")
    def main_extraction(self):
        df = self.get_data_table()
        return df, self.video_id, self.channel_id

""" ________________________________________________________________  Main  ________________________________________________________________"""

if __name__ == "__main__":
    data, id_video, id_channel = Extraction.main_extraction()
    # data = get_data_table(get_data())
