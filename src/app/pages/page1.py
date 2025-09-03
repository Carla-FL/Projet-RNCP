"""_________________________________ Dashboard Page 1 ___________________________________"""
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


def initialize_session_state():
    """Initialise les variables de session si elles n'existent pas"""
    if 'analysis_done' not in st.session_state:
        st.session_state.analysis_done = False
    if 'video_data' not in st.session_state:
        st.session_state.video_data = {}
    if 'url_input' not in st.session_state:
        st.session_state.url_input = ""


def make_wordcloud(text, where=None):
    """
    Génère un nuage de mots à partir d'un texte donné.
    
    Args:
        text (str): Le texte à analyser pour le nuage de mots.
    
    Returns:
        None
    """
    wordcloud = WordCloud(width=800, height=400, background_color='white', collocations=False).generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    # plt.show()
    if where:
        where.pyplot(plt)
    else:
        st.pyplot(plt)


def display_analysis_results():
    """Affiche les résultats de l'analyse stockés dans session_state"""
    if not st.session_state.analysis_done or not st.session_state.video_data:
        return False
    
    # Récupérer les données stockées
    data = st.session_state.video_data
    
    # Afficher le titre de la vidéo analysée
    st.header(f"Analyse de la vidéo :")
    st.subheader(f"{data['video_title']}")
    
    # Afficher la vidéo
    st.video(data['url'])
    
    # Afficher la date de mise à jour
    st.caption(f"Date de dernière mise à jour : {data['sync_date']}")
    st.header("Analyse de premier niveau")
    
    # Créer les colonnes
    left, right = st.columns([1,1], border=False)
    
    # Afficher les métriques
    left.metric(label="Nombre total de commentaires", value=data['total_comments'], border=True)
    
    # Container pour le commentaire le plus aimé
    left_container = left.container(border=True)
    left_container.header("Commentaire le plus aimé")
    left_container.subheader(data['most_liked_comment'])
    left_container.caption(f"Publié le : {data['most_liked_date']}")
    left_container.caption(f"Nombre de likes : {data['like_count']}")
    
    # Générer le wordcloud
    if data['wordcloud_text']:
        make_wordcloud(data['wordcloud_text'], right)
    
    # Graphique des commentaires par jour
    if data['comments_per_day']:
        import plotly.express as px
        fig = px.bar(x=data['comments_per_day']['dates'], 
                    y=data['comments_per_day']['counts'],
                    title='Nombre de commentaires par jour',
                    labels={'x': 'Date', 'y': 'Nombre de commentaires'})
        st.plotly_chart(fig, use_container_width=True)
    
    return True


def perform_analysis(url):
    """Effectue l'analyse et stocke les résultats dans session_state"""
    try:
        extract = Extraction(video_url=url)
        videoid = extract.url2id()
        st.session_state['videoid'] = videoid
    except Exception as e:
        st.error(f"Erreur veuillez entrer une URL YouTube valide. : {e}")
        return False
    
    client = Load().data_base_connexion()
    st.session_state['client_mdb'] = client
    
    try:
        db_name = get_existing_db(client, videoid)
        st.session_state['data_base_name'] = str(db_name)
    except Exception as e:
        st.error(f"Erreur lors de get_existing_db : {e}")
        return False
    
    data_exists = False
    try:
        db = client[db_name]
        if videoid in db.list_collection_names():
            st.success("La vidéo existe déjà dans la base de données")
            data_exists = True
    except Exception as e:
        data_exists = False
        st.error(f"Les données n'existent pas encore, on lance l'extraction des données ...")
        
        if not data_exists:
            with st.spinner("Wait for it...", show_time=True):
                chanel_id = main_etl(url, with_channel_id=True)
                st.success("Done!")
            
            try:
                db = client[chanel_id]
            except Exception as e:
                st.error(f"Erreur lors de l'extraction du channel_id : {e}")
                return False
    
    if db is not None:
        # Récupérer les KPI
        video_title, sync_date, total_comments, most_liked_comment, most_liked_date, like_count = get_kpi(db, videoid)
        
        # Préparer les données du DataFrame
        df = pd.DataFrame(list(db[videoid].find()))
        text = df['comment_clean_lem'].dropna()
        wordcloud_text = " ".join(text)
        
        # Données pour le graphique
        df_chart = pd.DataFrame(list(db[videoid].find(
            {},
            {"publishedAt": 1, "comment": 1, "_id": 0}
        )))
        df_chart['date'] = pd.to_datetime(df_chart['publishedAt']).dt.date
        comments_per_day = df_chart['date'].value_counts().sort_index()
        
        # Stocker toutes les données dans session_state
        st.session_state.video_data = {
            'url': url,
            'video_title': video_title,
            'sync_date': sync_date,
            'total_comments': total_comments,
            'most_liked_comment': most_liked_comment,
            'most_liked_date': most_liked_date,
            'like_count': like_count,
            'wordcloud_text': wordcloud_text,
            'comments_per_day': {
                'dates': comments_per_day.index.tolist(),
                'counts': comments_per_day.values.tolist()
            }
        }
        
        st.session_state.analysis_done = True
        Load().data_base_deconnexion(client)
        return True
    
    return False


# @st.cache_data
def main():
    # Initialiser les variables de session
    initialize_session_state()
    
    # Titre
    st.title("YOU REVIEW ")
    st.subheader("L'Analyse automatisée de vos commentaires YouTube", divider="gray")
    
    # Formulaire pour l'URL
    url = st.text_input("L'url de la vidéo", value=st.session_state.url_input, key="url")
    st.session_state['url_input'] = url
    
    # Boutons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        analyze_button = st.button("Lancer l'analyse")
    
    with col2:
        if st.session_state.analysis_done:
            clear_button = st.button("Effacer les résultats")
            if clear_button:
                st.session_state.analysis_done = False
                st.session_state.video_data = {}
                st.session_state.url_input = ""
                st.rerun()
    
    # Si on clique sur "Lancer l'analyse"
    if analyze_button:
        if not url:
            st.error("Le champ de l'URL ne peut pas être vide.")
            return
        
        if perform_analysis(url):
            st.rerun()  # Recharger la page pour afficher les résultats
    
    # Afficher les résultats s'ils existent
    if st.session_state.analysis_done:
        display_analysis_results()
    
    # Message d'aide si pas d'analyse
    #elif not st.session_state.analysis_done and not analyze_button:
        #st.info("👆 Entrez une URL YouTube et cliquez sur 'Lancer l'analyse' pour commencer.")


    # # Variable pour savoir si on doit faire l'extraction
    # data_exists = False
    # db = None
    # # titre 
    # st.title("YOU REVIEW ")
    # st.subheader("L'Analyse automatisé de vos commentaires YouTube", divider="gray")
    # # formulaire pour l'url de la vidéo et le pseudo du youtubeur
    # url = st.text_input("L'url de la vidéo", key="url")
    # # si l'url est dans le session state, on la récupère
    # st.session_state['url_input'] = url

    # if st.button("Lancer l'analyse"):
    #     if not url:
    #         st.error("le champ de l'URL ne peut pas être vide.")   
    #         return 
    #     try:
    #         extract = Extraction(video_url=url)
    #         videoid = extract.url2id()
    #         st.session_state['videoid'] = videoid
    #     except Exception as e:
    #         st.error(f"Erreur veuillez entrer une URL You Tube valide. : {e}")
    #         raise Exception(f"Erreur veuillez entrer une URL You Tube valide. : {e}")
        
    #     client = Load().data_base_connexion()
    #     st.session_state['client_mdb'] = client

    #     try:
    #         db_name = get_existing_db(client, videoid)
    #         st.session_state['data_base_name'] = str(db_name)
    #     except Exception as e:
    #         st.error(f"Erreur lors de get_existing_db : {e}")
    #         raise Exception(f"Erreur lors de get_existing_db : {e}")

    #     try:
    #         db = client[db_name]
    #         if videoid in db.list_collection_names(): # and db[videoid].count_documents({}) > 0:
    #             st.success("La video existe déjà dans la base de données")
    #             data_exists = True
    #     except Exception as e:
    #         data_exists = False
    #         st.error(f"Les données n'existent pas encore, on lance l'extration des données ...")
        
    #         if not data_exists:
    #             with st.spinner("Wait for it...", show_time=True):
    #                 chanel_id = main_etl(url, with_channel_id=True)
    #                 st.success("Done!")
    #         # st.success("Extraction terminée avec succès !")
    #         try:
    #             db = client[chanel_id]

    #         except Exception as e:
    #             st.error(f"Erreur lors de l'extraction du channel_id : {e}")
    #             raise Exception(f"Erreur lors de l'extraction du channel_id : {e}")

    #     if db is not None:
            
    #         viedo_title, synchronisation_date, total_comments, most_liked_comment, most_liked_comment_publish_date, like_count = get_kpi(db, videoid)
    #         # afficher le titre de la vidéo analysée
    #         st.header(f"Analyse de la vidéo :\n ")
    #         st.subheader(f"{viedo_title}")
    #         # afficher la video :
    #         st.video(url)
    #         # afficher la date de mise à jours des données 
    #         st.caption(f"Date de dernière mise à jour : {synchronisation_date}")
    #         st.header("Analyse de premier niveau")

    #         # création de colonnes
    #         left, right = st.columns([1,1], border=False)
            

    #         # afficher le nombre de commentaires
    #         left.metric(label="Nombre total de commentaires", value= db[videoid].count_documents({}), border=True)
    #         # afficher le commentaire avec le plus de likes
    #         # most_liked_comment = db[videoid].find_one(sort=[("likeCount", -1)])["comment"]
    #         most_liked_comment = most_liked_comment if most_liked_comment else "Aucun commentaire trouvé"
    #         # st.metric(label="Commentaire le plus aimé", value=str(most_liked_comment),border=True)

    #         # with left.container(border=True):
    #         left_container = left.container(border=True)
    #         left_container.header("Commentaire le plus aimé")
    #         left_container.subheader(most_liked_comment)
    #         left_container.caption(f"Publié le : {most_liked_comment_publish_date}")
    #         left_container.caption(f"Nombre de likes : {like_count}")

            
    #         df = pd.DataFrame(list( db[videoid].find()))
    #         # text = df['tokens_clean_lem'].dropna().astype(str).str.cat(sep=" ")
    #         text = df['comment_clean_lem'].dropna()
    #         make_wordcloud(" ".join(text), right)

    #         # afficher le commentaire le plus long
    #         # afficher l'évolution du nombre de commentaires au fil du temps
    #         # df = pd.DataFrame(list(db[videoid].find(
    #         #     {},                      # filtre : vide pour tous les documents
    #         #     {"publishedAt": 1, "comment": 1, "_id": 0}  # projection : 1 pour inclure, 0 pour exclure _id
    #         # )))
    #         # # le nombre de commentaires par jour
    #         # # Conversion en date simple
    #         # df['date'] = pd.to_datetime(df['publishedAt']).dt.date
    #         # # Création de la figure
    #         # fig, ax = plt.subplots(figsize=(12, 6))
    #         # df['date'].value_counts().sort_index().plot(kind='bar', ax=ax)
    #         # # Titres et labels
    #         # ax.set_title('Nombre de commentaires par jour')
    #         # ax.set_xlabel('Date')
    #         # ax.set_ylabel('Nombre de commentaires')
    #         # plt.xticks(rotation=45)
    #         # plt.tight_layout()
    #         # # Affichage dans Streamlit
    #         # st.pyplot(fig)

    #         # Le nombre de commentaires par jour avec l'heure
    #         # Le nombre de commentaires par jour
    #         df = pd.DataFrame(list(db[videoid].find(
    #             {}, # filtre : vide pour tous les documents
    #             {"publishedAt": 1, "comment": 1, "_id": 0} # projection : 1 pour inclure, 0 pour exclure _id
    #         )))

    #         # Conversion en date simple
    #         df['date'] = pd.to_datetime(df['publishedAt']).dt.date

    #         # Compter les commentaires par jour
    #         comments_per_day = df['date'].value_counts().sort_index()

    #         # Création de l'histogramme avec Plotly
    #         import plotly.express as px

    #         fig = px.bar(x=comments_per_day.index, 
    #                     y=comments_per_day.values,
    #                     title='Nombre de commentaires par jour',
    #                     labels={'x': 'Date', 'y': 'Nombre de commentaires'})

    #         # Affichage dans Streamlit
    #         st.plotly_chart(fig, use_container_width=True)

            

    #         Load().data_base_deconnexion(client)



        # mentions légales
        # politque de confidentielité
        # condition gérale d'utilisation

        # doc de formation
        # doc technique