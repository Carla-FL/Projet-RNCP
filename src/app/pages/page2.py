import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
import streamlit as st
import json
import os
from src.Pipeline1.etl import main_etl
import matplotlib.pyplot as plt
import pandas as pd
from src.utils.load import Load
from src.utils.extraction import Extraction
import plotly.express as px


def sentiment_kpi(client, db, videoid):

    collection = client[db][videoid]
    # graphique camembert de répartition du nombre de commentaire positifs et négatifs et neutres
    sentiments = collection.aggregate([
        {"$group": {
            "_id": "$sentiment",
            "count": {"$sum": 1}
        }}
    ])
    sentiment_counts = {sentiment['_id']: sentiment['count'] for sentiment in sentiments}
    sentiment_labels = list(sentiment_counts.keys())
    sentiment_values = list(sentiment_counts.values())
    # Création du DataFrame pour Plotly
    df = pd.DataFrame({
        "Sentiment": sentiment_labels,
        "Valeur": sentiment_values
    })
    fig = px.pie(df, values="Valeur", names="Sentiment", title="Répartition des sentiments")
    st.plotly_chart(fig, use_container_width=True)


    # graphique de l'évolution des sentiments au fil du temps
    sentiments_time = collection.aggregate([
        {"$group": {
            "_id": {"sentiment": "$sentiment", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$publishedAt"}}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": 1}}
    ])
    sentiment_time_data = []
    for sentiment in sentiments_time:
        sentiment_time_data.append({
            "date": sentiment['_id']['date'],
            "sentiment": sentiment['_id']['sentiment'],
            "count": sentiment['count']
        })
    sentiment_time_df = pd.DataFrame(sentiment_time_data)
    # Création du graphique
    fig_time = px.line(sentiment_time_df, x="date", y="count", color="sentiment", title="Évolution des sentiments au fil du temps")
    fig_time.update_xaxes(title_text="Date")
    fig_time.update_yaxes(title_text="Nombre de commentaires")
    st.plotly_chart(fig_time, use_container_width=True)

    # Affichage dans Streamlit
    

def topic_modeling(client, db, videoid):
    doc = client[db][videoid].find({}, {"comment": 1, "sentiment":1, "_id": 0})
    df = pd.DataFrame(list(doc))
    
    pass

def main():
    url = st.session_state['url_input']
    # st.write(f"URL de la vidéo analysée : {url}")
    videoid = st.session_state.get("videoid", None)
    # st.write(f"ID de la vidéo analysée : {videoid}")
    db = st.session_state.get('data_base_name', None)
    # st.write(f"Nom de la base de données : {db}")
    client = Load().data_base_connexion()

    sentiment_kpi(client, db, videoid)
    
    # st.write(f"URL de la vidéo analysée :", url)

# mettre des filtres pour afficher une partie du df et lancer le topic modeling dessus 
# filtre date
# filtre sentiment
# filtre nombre de likes


# faire les graphique pour présenter les résultats l'analyse de sentiment
# ajouter un filtre pour la base de données sur la valeur du sentiment
# ajouter une fonction qui prend en entré le jeu de données filtré et fait le topic modeling dessus
# afficher les résultats du topic modeling
# ajouter la possibilité d'extraire les résultats sour forme de pdf