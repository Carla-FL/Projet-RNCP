#"""_________________________________ Dashboard Page 1 ___________________________________"""
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



def eda(url):
    try :
        chanel = Extraction(url).channel_id
    except Exception as e:
        st.error(f"Erreur lors de l'extraction du channel_id : {e}")
    try:
        videoid = Extraction(video_url=url).video_id
        client = Load().data_base_connexion()
    except Exception as e:
        st.error(f"Erreur lors de l'extraction de l'ID de la vidéo : {e}")
    db = client[chanel]

    # afficher le titre de la vidéo analysée
    st.header(f"Analyse de la vidéo :")
    st.subheader(f"{db[videoid].find_one(sort=[('publishedAt', -1)])['titre']}")
    # afficher la date de mise à jours des données 
    st.caption(f"Date de dernière mise à jours : {db[videoid].find_one(sort=[('extractedAt', -1)])['extractedAt']}")

    st.header("Analyse de premier niveau")
    # afficher le nombre de commentaires
    st.metric(label="Nombre total de commentaires", value= db[videoid].count_documents({}), border=True)
    # afficher le commentaire avec le plus de likes
    most_liked_comment = db[videoid].find_one(sort=[("likeCount", -1)])
    st.metric(label="Commentaire le plus aimé", value=most_liked_comment,border=True)
    # afficher le commentaire le plus long
    # afficher l'évolution du nombre de commentaires au fil du temps
    df = pd.DataFrame(list(db[videoid].find(
        {},                      # filtre : vide pour tous les documents
        {"publishedAt": 1, "comment": 1, "_id": 0}  # projection : 1 pour inclure, 0 pour exclure _id
    )))
    # le nombre de commentaires par jour
    # Conversion en date simple
    df['date'] = pd.to_datetime(df['publishedAt']).dt.date
    # Création de la figure
    fig, ax = plt.subplots(figsize=(12, 6))
    df['date'].value_counts().sort_index().plot(kind='bar', ax=ax)
    # Titres et labels
    ax.set_title('Nombre de commentaires par jour')
    ax.set_xlabel('Date')
    ax.set_ylabel('Nombre de commentaires')
    plt.xticks(rotation=45)
    plt.tight_layout()
    # Affichage dans Streamlit
    st.pyplot(fig)
    Load().data_base_deconnexion(client)

def get_existing_db(client, videoid):
    for db_name in client.list_database_names():
        if db_name in ("admin", "config", "local"):  # filtrer les bases internes MongoDB
            continue
        db = client[db_name]
        # Lister toutes les collections de cette base
        collections = db.list_collection_names()
        if videoid in collections:
            return db.name
        #else:
            #raise Exception("La vidéo n'existe pas dans la base de données. Veuillez lancer l'extraction des données.")


def get_kpi(db, videoid):
    viedo_title = db[videoid].find_one(sort=[('publishedAt', -1)])['titre']
    synchronisation_date = db[videoid].find_one(sort=[('extractedAt', -1)])['extractedAt']
    total_comments = db[videoid].count_documents({})
    most_liked_comment = db[videoid].find_one(sort=[("likeCount", -1)])["comment"]
    like_count = db[videoid].find_one(sort=[("likeCount", -1)])["likeCount"]
    
    return viedo_title, synchronisation_date, total_comments, most_liked_comment, like_count


def main():
    # Variable pour savoir si on doit faire l'extraction
    data_exists = False
    db = None
    # titre 
    st.title("YOU REVIEW ")
    st.subheader("L'Analyse automatisé de vos commentaires YouTube")
    # formulaire pour l'url de la vidéo et le pseudo du youtubeur
    url = st.text_input("L'url de la vidéo", key="url")
    st.session_state['url_input'] = url

    if st.button("Lancer l'analyse"):
        if not url:
            st.error("le champ de l'URL ne peut pas être vide.")   

        try:
            videoid = Extraction(video_url=url).url2id()
            st.session_state['videoid'] = videoid
        except Exception as e:
            st.error(f"Erreur veuillez entrer une URL youyube valide. : {e}")
        
        client = Load().data_base_connexion()
        st.session_state['client_mdb'] = client

        try:
            db_name = get_existing_db(client, videoid)
            st.session_state['data_base_name'] = db_name
        except Exception as e:
            st.error(f"Erreur lors de get_existing_db : {e}")

        try:
            db = client[db_name]
            if videoid in db.list_collection_names(): # and db[videoid].count_documents({}) > 0:
                st.success("La video existe déjà dans la base de données")
                data_exists = True
        except Exception as e:
            data_exists = False
            st.error(f"Les données n'existent pas encore, on lance l'extration des données ...")

            if not data_exists:
                with st.spinner("Wait for it...", show_time=True):
                    chanel_id = main_etl(url, with_channel_id=True)
                    st.success("Done!")
            # st.success("Extraction terminée avec succès !")
            try:
                db = client[chanel_id]
            except Exception as e:
                st.error(f"Erreur lors de l'extraction du channel_id : {e}")
                return

        if db is not None:
            
            viedo_title, synchronisation_date, total_comments, most_liked_comment, like_count = get_kpi(db, videoid)
            # afficher le titre de la vidéo analysée
            st.header(f"Analyse de la vidéo :\n ")
            st.subheader(f"{viedo_title}")
            # afficher la video :
            st.video(url)
            # afficher la date de mise à jours des données 
            st.caption(f"Date de dernière mise à jour : {db[videoid].find_one(sort=[('extractedAt', -1)])['extractedAt']}")
            st.header("Analyse de premier niveau")
            # afficher le nombre de commentaires
            st.metric(label="Nombre total de commentaires", value= db[videoid].count_documents({}), border=True)
            # afficher le commentaire avec le plus de likes
            # most_liked_comment = db[videoid].find_one(sort=[("likeCount", -1)])["comment"]
            most_liked_comment = most_liked_comment if most_liked_comment else "Aucun commentaire trouvé"
            st.metric(label="Commentaire le plus aimé", value=str(most_liked_comment),border=True)
            # afficher le commentaire le plus long
            # afficher l'évolution du nombre de commentaires au fil du temps
            df = pd.DataFrame(list(db[videoid].find(
                {},                      # filtre : vide pour tous les documents
                {"publishedAt": 1, "comment": 1, "_id": 0}  # projection : 1 pour inclure, 0 pour exclure _id
            )))
            # le nombre de commentaires par jour
            # Conversion en date simple
            df['date'] = pd.to_datetime(df['publishedAt']).dt.date
            # Création de la figure
            fig, ax = plt.subplots(figsize=(12, 6))
            df['date'].value_counts().sort_index().plot(kind='bar', ax=ax)
            # Titres et labels
            ax.set_title('Nombre de commentaires par jour')
            ax.set_xlabel('Date')
            ax.set_ylabel('Nombre de commentaires')
            plt.xticks(rotation=45)
            plt.tight_layout()
            # Affichage dans Streamlit
            st.pyplot(fig)
            Load().data_base_deconnexion(client)



    # mentions légales
    # politque de confidentielité
    # condition gérale d'utilisation

    # doc de formation
    # doc technique