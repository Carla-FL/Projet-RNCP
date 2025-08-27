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
from wordcloud import WordCloud


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
    most_liked_comment_publish_date = db[videoid].find_one(sort=[("likeCount", -1)])["publishedAt"]
    like_count = db[videoid].find_one(sort=[("likeCount", -1)])["likeCount"]
    
    return viedo_title, synchronisation_date, total_comments, most_liked_comment, most_liked_comment_publish_date, like_count



def make_wordcloud(text, where=None):
    """
    Génère un nuage de mots à partir d'un texte donné.
    
    Args:
        text (str): Le texte à analyser pour le nuage de mots.
    
    Returns:
        None
    """
    # wordcloud = WordCloud(
    #     width=600, 
    #     height=300, 
    #     background_color=None,  # Fond transparent
    #     mode='RGBA',  # Mode pour transparence
    #     collocations=False,
    #     max_words=100,  # Limite le nombre de mots
    #     relative_scaling=0.5,  # Réduit la différence de taille entre mots
    #     colormap='viridis'  # Palette de couleurs
    # ).generate(text)
    
    # fig, ax = plt.subplots(figsize=(8, 4))
    # ax.imshow(wordcloud, interpolation='bilinear')
    # ax.axis('off')
    
    # # Supprime les marges blanches autour du nuage
    # plt.tight_layout(pad=0)
    # plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    
    # st.pyplot(fig, bbox_inches='tight', pad_inches=0)
    wordcloud = WordCloud(width=800, height=400, background_color='white', collocations=False).generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    # plt.show()
    if where:
        where.pyplot(plt)
    else:
        st.pyplot(plt)


def main():
    # Variable pour savoir si on doit faire l'extraction
    data_exists = False
    db = None
    # titre 
    st.title("YOU REVIEW ")
    st.subheader("L'Analyse automatisé de vos commentaires YouTube", divider="gray")
    # formulaire pour l'url de la vidéo et le pseudo du youtubeur
    url = st.text_input("L'url de la vidéo", key="url")
    # si l'url est dans le session state, on la récupère
    st.session_state['url_input'] = url

    if st.button("Lancer l'analyse"):
        if not url:
            st.error("le champ de l'URL ne peut pas être vide.")   
            return 
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
            
            viedo_title, synchronisation_date, total_comments, most_liked_comment, most_liked_comment_publish_date, like_count = get_kpi(db, videoid)
            # afficher le titre de la vidéo analysée
            st.header(f"Analyse de la vidéo :\n ")
            st.subheader(f"{viedo_title}")
            # afficher la video :
            st.video(url)
            # afficher la date de mise à jours des données 
            st.caption(f"Date de dernière mise à jour : {synchronisation_date}")
            st.header("Analyse de premier niveau")

            # création de colonnes
            left, right = st.columns([1,1], border=False)
            

            # afficher le nombre de commentaires
            left.metric(label="Nombre total de commentaires", value= db[videoid].count_documents({}), border=True)
            # afficher le commentaire avec le plus de likes
            # most_liked_comment = db[videoid].find_one(sort=[("likeCount", -1)])["comment"]
            most_liked_comment = most_liked_comment if most_liked_comment else "Aucun commentaire trouvé"
            # st.metric(label="Commentaire le plus aimé", value=str(most_liked_comment),border=True)

            # with left.container(border=True):
            left_container = left.container(border=True)
            left_container.header("Commentaire le plus aimé")
            left_container.subheader(most_liked_comment)
            left_container.caption(f"Publié le : {most_liked_comment_publish_date}")
            left_container.caption(f"Nombre de likes : {like_count}")

            
            df = pd.DataFrame(list( db[videoid].find()))
            # text = df['tokens_clean_lem'].dropna().astype(str).str.cat(sep=" ")
            text = df['comment_clean_lem'].dropna()
            make_wordcloud(" ".join(text), right)

            # afficher le commentaire le plus long
            # afficher l'évolution du nombre de commentaires au fil du temps
            # df = pd.DataFrame(list(db[videoid].find(
            #     {},                      # filtre : vide pour tous les documents
            #     {"publishedAt": 1, "comment": 1, "_id": 0}  # projection : 1 pour inclure, 0 pour exclure _id
            # )))
            # # le nombre de commentaires par jour
            # # Conversion en date simple
            # df['date'] = pd.to_datetime(df['publishedAt']).dt.date
            # # Création de la figure
            # fig, ax = plt.subplots(figsize=(12, 6))
            # df['date'].value_counts().sort_index().plot(kind='bar', ax=ax)
            # # Titres et labels
            # ax.set_title('Nombre de commentaires par jour')
            # ax.set_xlabel('Date')
            # ax.set_ylabel('Nombre de commentaires')
            # plt.xticks(rotation=45)
            # plt.tight_layout()
            # # Affichage dans Streamlit
            # st.pyplot(fig)

            # Le nombre de commentaires par jour avec l'heure
            # Le nombre de commentaires par jour
            df = pd.DataFrame(list(db[videoid].find(
                {}, # filtre : vide pour tous les documents
                {"publishedAt": 1, "comment": 1, "_id": 0} # projection : 1 pour inclure, 0 pour exclure _id
            )))

            # Conversion en date simple
            df['date'] = pd.to_datetime(df['publishedAt']).dt.date

            # Compter les commentaires par jour
            comments_per_day = df['date'].value_counts().sort_index()

            # Création de l'histogramme avec Plotly
            import plotly.express as px

            fig = px.bar(x=comments_per_day.index, 
                        y=comments_per_day.values,
                        title='Nombre de commentaires par jour',
                        labels={'x': 'Date', 'y': 'Nombre de commentaires'})

            # Affichage dans Streamlit
            st.plotly_chart(fig, use_container_width=True)

            

            Load().data_base_deconnexion(client)



        # mentions légales
        # politque de confidentielité
        # condition gérale d'utilisation

        # doc de formation
        # doc technique