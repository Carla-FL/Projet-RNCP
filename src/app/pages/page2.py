import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import json
import os
from src.Pipeline1.etl import main_etl
import matplotlib.pyplot as plt
import pandas as pd
from src.utils.load import Load
from src.utils.extraction import Extraction
import plotly.express as px
from src.utils.topicmodeling import TopicModeling
from src.utils.redis_cahce import get_redis_client


def topic_kpi(df):
    topic_counts = df.groupby(['topic_keywords']).size().reset_index(name='count')

    # Trier par ordre décroissant pour une meilleure visualisation
    topic_counts = topic_counts.sort_values(by='count', ascending=False)

    # Afficher le tableau dans Streamlit
    st.dataframe(topic_counts)

    # Création du graphique camembert avec Plotly
    fig = px.pie(topic_counts, 
                values='count', 
                names='topic_keywords',
                title='Répartition des documents par topic')

    # Personnalisation du graphique
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>' +
                    'Nombre de documents: %{value}<br>' +
                    'Pourcentage: %{percent}<br>' +
                    '<extra></extra>'
    )

    # Ajuster la taille et la disposition
    fig.update_layout(
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.01
        )
    )

    # Affichage dans Streamlit
    st.plotly_chart(fig, use_container_width=True)


def sentiment_kpi(client, db, videoid):

    collection = client[db][videoid]
    # graphique camembert de répartition du nombre de commentaire positifs et négatifs et neutres
    sentiments = collection.aggregate([
        {"$group": {
            "_id": "$sentiment",
            "count": {"$sum": 1}
        }}
    

    ])
    # st.dataframe(pd.DataFrame(sentiments))

    sentiment_counts = {sentiment['_id']: sentiment['count'] for sentiment in sentiments}
    sentiment_labels = list(sentiment_counts.keys())
    sentiment_values = list(sentiment_counts.values())

    # Création du DataFrame pour Plotly
    df = pd.DataFrame({
        "Sentiment": sentiment_labels,
        "Valeur": sentiment_values
    })

    # color_map = {'positive': '#28a745','negative': '#dc3545', 'neutral': '#ffc107','mixed': '#6c757d'}
    color_map = {'positive': 'green','negative': 'red', 'neutral': 'grey','mixed': 'grey'}
    # colors = [color_map.get(sentiment, 'grey') for sentiment in sentiment_labels]
    # st.write(color_map)

    fig = px.pie(df, values="Valeur", names="Sentiment", title="Répartition des sentiments", color="Sentiment", color_discrete_map=color_map)

    st.plotly_chart(fig, use_container_width=True)


def topic_modeling(client, db, videoid):
    doc = client[db][videoid].find({}, {"comment": 1, "sentiment":1, "_id": 0})
    df = pd.DataFrame(list(doc))
    
    pass


def sentiment_choice():
    options = st.multiselect(
    "Quel sentiment souhaitez-vous afficher ?",
    ["positive", "negative", "neutral"]
)
    return options


def exemple_data (client,db,  videoid, sentiments:list=None):
    st.write("Exemple de données pour les sentiments sélectionnés :")
    collection = client[db][videoid]
    df = pd.DataFrame(list(collection.find({})))
    # selectionner les coolonnes commentaire et sentiment
    df = df[['comment', 'sentiment']]
    # st.write(f"Nombre de commentaires : {len(df)}")

    for sent in sentiments:
        if sent == "positive":
            dfpos = df[df["sentiment"] == 2].iloc[:3]
            st.write(f"Nombre de commentaires positifs : {len(dfpos)}")
            st.dataframe(dfpos, use_container_width=True)
        if sent == "negative":
            dfneg = df[df["sentiment"] == 0].iloc[:3]
            st.write(f"Nombre de commentaires négatifs : {len(dfneg)}")
            st.dataframe(dfneg, use_container_width=True)
        if sent == "neutral":
            dfnet = df[df["sentiment"] == 1].iloc[:3]
            st.write(f"Nombre de commentaires neutres : {len(dfnet)}")
            st.dataframe(dfnet, use_container_width=True)


def main():
    url = st.session_state['url_input']
    # st.write(f"URL de la vidéo analysée : {url}")
    videoid = st.session_state.get("videoid", None)
    # st.write(f"ID de la vidéo analysée : {videoid}")
    db = st.session_state.get('data_base_name', None)
    # st.write(f"Nom de la base de données : {db}")
    client = Load().data_base_connexion()

    redis_client = get_redis_client()
    # if redis_client:
    #     with st.expander("Statut du Cache Redis"):
    #         try:
    #             info = redis_client.info('memory')
    #             col1, col2 = st.columns(2)
    #             with col1:
    #                 st.metric("Mémoire Redis", f"{info['used_memory'] / (1024*1024):.1f} MB")
    #             with col2:
    #                 keys = redis_client.keys("topic_model:*")
    #                 st.metric("Modèles en cache", len(keys))
    #         except:
    #             st.info("Informations Redis non disponibles")

    sentiment_kpi(client, db, videoid)

    sentiments = sentiment_choice()
    st.write("Exemple de données pour les sentiments sélectionnés :")
    
    collection = client[db][videoid]
    df = pd.DataFrame(list(collection.find({})))
    # selectionner les coolonnes commentaire et sentiment
    #df = df[['comment', 'sentiment']]
    st.write(f"Nombre de commentaires : {len(df)}")
    # st.dataframe(df.head(), use_container_width=True)

    if "positive" in sentiments:

        dfpos = df[["comment", "sentiment"]][df["sentiment"] == "positive"].iloc[:3]
        st.write(f"Exemple de données pour les commentaires positifs ")
        st.dataframe(dfpos, use_container_width=True)
    if "negative" in sentiments:
        dfneg = df[["comment", "sentiment"]][df["sentiment"] == "negative"].iloc[:3]
        st.write("Exemple de données pour les commentaires négatifs ")
        st.dataframe(dfneg, use_container_width=True)
    if "neutral" in sentiments:
        st.write("Exemple de données pour les commentaires neutres ")
        dfnet = df[["comment", "sentiment"]][df["sentiment"] == "neutral"].iloc[:3]
        st.dataframe(dfnet, use_container_width=True)

    st.write("Topic modeling sur les données selectionnées")
    
    if st.button("prêt pour le topic modeling"):
        # st.write(sentiments)
        try :
            data = df[df["sentiment"].isin(sentiments)]
            st.write(f"Données pour le topic modeling 1: {len(df)}")
        except KeyError as e:
            st.error(f"Erreur lors de la sélection des sentiments : {e}")

        try:
            tp = TopicModeling(data, videoid=videoid, sentiments=sentiments)
            # st.write(f"Données pour le topic modeling : {len(data)}")
            # st.write(len(tp.corpus))
            # st.write(len(tp.dictionary))
            # st.write(len(tp.corpus))
            
            with st.spinner("Wait for it...", show_time=True):
                result = tp.main_topic_modeling()
            st.success(f"Résultats du topic modeling : {len(result)} commentaires analysés")

            # graphique de distribution des commentaires par topic
            st.write("Distribution des commentaires par topic :")
            topic_kpi(result)

        except Exception as e:
            st.error(f"Erreur lors du topic modeling : {e}")

# ajouter un filtre pour la base de données sur la valeur du sentiment
# ajouter une fonction qui prend en entré le jeu de données filtré et fait le topic modeling dessus
# afficher les résultats du topic modeling
# ajouter la possibilité d'extraire les résultats sour forme de pdf