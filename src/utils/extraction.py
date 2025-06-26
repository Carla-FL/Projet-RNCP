""" ________________________________________________________________   Library   ________________________________________________________________"""
import re
import time
import pandas as pd
from googleapiclient.discovery import build
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

""" ________________________________________________________________  Fonctions  ________________________________________________________________"""

def url2id():
    video_url = input("Enter the video URL: ")
    pattern = r'^(https?:\/\/)?(www\.)?(youtube\.com\/(watch\?v=|embed\/|v\/)|youtu\.be\/)([\w-]{11})(\S+)?$'
    if re.match(pattern, video_url) :
        print("Valid YouTube URL")
    else:
        print("Error : invalid YouTube URL")

    video_id = video_url.split("v=")[-1]
    # print("Extracted video ID:", video_id)

    # Check if the video ID is valid
    if len(video_id) == 11:
        print("Valid video ID:", video_id)
        return video_id
    else:
        print("Error : invalid video ID. Please enter a valid YouTube video URL.")


""" ________________________________________________________________  Main  ________________________________________________________________"""

if __name__ == "__main__":

    id=url2id()

    with open('secret_clientid.txt', 'r') as file:
            DEVELOPER_KEY = file.read()
    # Configurez votre clé API et initialisez l'API YouTube
    api_key = DEVELOPER_KEY
    youtube = build("youtube", "v3", developerKey=api_key)

    # Paramètres initiaux pour la requête
    video_id = id
    comments_data = []
    next_page_token = None

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
            comment_info = item["snippet"]["topLevelComment"]["snippet"]
            comments_data.append({
                "channelId": comment_info.get("channelId"),
                "videoId": comment_info.get("videoId"),	
                "author": comment_info.get("authorDisplayName"),
                "publishedAt": comment_info.get("publishedAt"),
                "comment": comment_info.get("textDisplay"),
                "extractedAt": extraction_date
            }) # ajouter un id

        # Vérifier s'il y a une page suivante
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
        time.sleep(1)

    # Création du DataFrame à partir des données collectées
    df = pd.DataFrame(comments_data)
    # print(df.head())
    # print(f"Total de commentaires récupérés : {len(comments_data)}")