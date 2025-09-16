"""_________________________________ Dashboard Page 1 ___________________________________"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import plotly.express as px

# Import conditionnel pour éviter les erreurs
try:
    from src.Pipeline1.etl import main_etl
    from src.utils.load import Load
    from src.utils.extraction import Extraction
except ImportError:
    try:
        from Pipeline1.etl import main_etl
        from utils.load import Load
        from utils.extraction import Extraction
    except ImportError:
        from src.Pipeline1.etl import main_etl
        from src.utils.load import Load
        from src.utils.extraction import Extraction

def get_existing_db(client, videoid):
    """Trouve la base contenant la vidéo - Compatible cloud/local"""
    try:
        # Mode cloud : une seule base
        from streamlit_config import get_database_connections
        db_config = get_database_connections()
        db_name = db_config["mongodb"]["database"]
        
        # Vérifier si la vidéo existe dans cette base
        db = client[db_name]
        collections = db.list_collection_names()
        
        # Chercher la collection qui contient cette vidéo
        for collection_name in collections:
            if videoid in collection_name:
                return db_name
                
    except ImportError:
        # Mode local : bases multiples
        for db_name in client.list_database_names():
            if db_name in ("admin", "config", "local", "test"):
                continue
            db = client[db_name]
            if videoid in db.list_collection_names():
                return db_name
    
    return None

def get_collection_name(db, videoid):
    """Détermine le nom de la collection selon l'architecture"""
    try:
        from streamlit_config import get_database_connections
        # Mode cloud : collection préfixée
        collections = db.list_collection_names()
        for coll in collections:
            if videoid in coll:
                return coll
        return None
    except ImportError:
        # Mode local : collection = videoid
        return videoid if videoid in db.list_collection_names() else None

def get_kpi(client, db_name, videoid):
    """Version robuste de get_kpi"""
    try:
        db = client[db_name]
        collection_name = get_collection_name(db, videoid)
        
        if not collection_name:
            st.error(f"Collection pour vidéo {videoid} non trouvée")
            return "Erreur", "Erreur", 0, "Erreur", "Erreur", 0
        
        collection = db[collection_name]
        
        # Vérifier qu'il y a des documents
        total_docs = collection.count_documents({})
        if total_docs == 0:
            st.error("Collection vide")
            return "Collection vide", "N/A", 0, "Aucun commentaire", "N/A", 0
        
        # Récupération sécurisée des données
        latest_doc = collection.find_one(sort=[('publishedAt', -1)])
        if not latest_doc:
            st.error("Aucun document trouvé")
            return "Erreur", "Erreur", 0, "Erreur", "Erreur", 0
        
        # Extraction sécurisée des champs
        video_title = latest_doc.get('titre', 'Titre non disponible')
        sync_date = latest_doc.get('extractedAt', 'Date inconnue')
        
        # Commentaire le plus aimé
        most_liked_doc = collection.find_one(
            {"likeCount": {"$exists": True, "$type": "number"}},
            sort=[("likeCount", -1)]
        )
        
        if most_liked_doc:
            most_liked_comment = most_liked_doc.get('comment', 'Commentaire non disponible')
            most_liked_date = most_liked_doc.get('publishedAt', 'Date inconnue')
            like_count = most_liked_doc.get('likeCount', 0)
        else:
            most_liked_comment = "Aucun commentaire avec likes"
            most_liked_date = "N/A"
            like_count = 0
        
        return video_title, sync_date, total_docs, most_liked_comment, most_liked_date, like_count
        
    except Exception as e:
        st.error(f"Erreur dans get_kpi: {str(e)}")
        return "Erreur", "Erreur", 0, "Erreur", "Erreur", 0

def prepare_chart_data(client, db_name, videoid):
    """Prépare les données pour le graphique de façon sécurisée"""
    try:
        db = client[db_name]
        collection_name = get_collection_name(db, videoid)
        
        if not collection_name:
            return {'dates': [], 'counts': []}
        
        collection = db[collection_name]
        
        # Récupérer les données avec projection
        cursor = collection.find(
            {"publishedAt": {"$exists": True, "$ne": None}},
            {"publishedAt": 1, "_id": 0}
        ).limit(10000)  # Limite pour éviter la surcharge
        
        df_chart = pd.DataFrame(list(cursor))
        
        if df_chart.empty or 'publishedAt' not in df_chart.columns:
            return {'dates': [], 'counts': []}
        
        # Conversion sécurisée des dates
        try:
            df_chart['datetime'] = pd.to_datetime(df_chart['publishedAt'], errors='coerce')
            df_chart = df_chart.dropna(subset=['datetime'])
            
            if df_chart.empty:
                return {'dates': [], 'counts': []}
            
            df_chart['date'] = df_chart['datetime'].dt.date
            comments_per_day = df_chart['date'].value_counts().sort_index()
            
            # Convertir en format compatible Plotly
            return {
                'dates': [str(date) for date in comments_per_day.index.tolist()],
                'counts': comments_per_day.values.tolist()
            }
            
        except Exception as e:
            st.warning(f"Erreur traitement dates: {e}")
            return {'dates': [], 'counts': []}
            
    except Exception as e:
        st.warning(f"Erreur préparation graphique: {e}")
        return {'dates': [], 'counts': []}

def prepare_wordcloud_data(client, db_name, videoid):
    """Prépare les données pour le wordcloud"""
    try:
        db = client[db_name]
        collection_name = get_collection_name(db, videoid)
        
        if not collection_name:
            return "Erreur de collection"
        
        collection = db[collection_name]
        
        # Essayer d'abord comment_clean_lem
        if collection.count_documents({"comment_clean_lem": {"$exists": True, "$ne": ""}}) > 0:
            cursor = collection.find(
                {"comment_clean_lem": {"$exists": True, "$ne": ""}},
                {"comment_clean_lem": 1, "_id": 0}
            ).limit(1000)
            
            texts = [doc.get('comment_clean_lem', '') for doc in cursor if doc.get('comment_clean_lem')]
            if texts:
                return " ".join(texts)
        
        # Fallback vers comment original
        cursor = collection.find(
            {"comment": {"$exists": True, "$ne": ""}},
            {"comment": 1, "_id": 0}
        ).limit(500)
        
        texts = [doc.get('comment', '') for doc in cursor if doc.get('comment')]
        if texts:
            return " ".join(texts)
        
        return "Aucun texte disponible"
        
    except Exception as e:
        st.warning(f"Erreur préparation wordcloud: {e}")
        return "Erreur préparation wordcloud"

def make_wordcloud(text, where=None):
    """Génère un nuage de mots sécurisé"""
    try:
        if not text or len(text.strip()) < 10:
            text = "Pas assez de texte pour générer un wordcloud"
        
        wordcloud = WordCloud(
            width=800, 
            height=400, 
            background_color='white', 
            collocations=False,
            max_words=100
        ).generate(text)
        
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title("Nuage de mots des commentaires")
        
        if where:
            where.pyplot(plt, clear_figure=True)
        else:
            st.pyplot(plt, clear_figure=True)
            
    except Exception as e:
        error_msg = f"Impossible de générer le wordcloud: {e}"
        if where:
            where.error(error_msg)
        else:
            st.error(error_msg)

def initialize_session_state():
    """Initialise les variables de session"""
    if 'analysis_done' not in st.session_state:
        st.session_state.analysis_done = False
    if 'video_data' not in st.session_state:
        st.session_state.video_data = {}
    if 'url_input' not in st.session_state:
        st.session_state.url_input = ""

def display_analysis_results():
    """Affiche les résultats avec gestion d'erreurs robuste"""
    if not st.session_state.analysis_done or not st.session_state.video_data:
        return False
    
    try:
        data = st.session_state.video_data
        
        # Vérification des données essentielles
        if not data.get('video_title'):
            st.error("Données de vidéo manquantes")
            return False
        
        # Affichage du titre
        st.header("Analyse de la vidéo :")
        st.subheader(f"{data['video_title']}")
        
        # Affichage de la vidéo
        if data.get('url'):
            st.video(data['url'])
        
        # Date de mise à jour
        if data.get('sync_date'):
            st.caption(f"Date de dernière mise à jour : {data['sync_date']}")
        
        st.header("Analyse de premier niveau")
        
        # Colonnes
        left, right = st.columns([1,1], border=False)
        
        # Métriques
        with left:
            st.metric("Nombre total de commentaires", data.get('total_comments', 0))
            
            # Container commentaire le plus aimé
            with st.container(border=True):
                st.subheader("Commentaire le plus aimé")
                st.write(data.get('most_liked_comment', 'Aucun'))
                st.caption(f"Publié le : {data.get('most_liked_date', 'N/A')}")
                st.caption(f"Likes : {data.get('like_count', 0)}")
        
        # Wordcloud
        with right:
            if data.get('wordcloud_text'):
                make_wordcloud(data['wordcloud_text'], right)
            else:
                st.info("Pas de données pour le wordcloud")
        
        # Graphique des commentaires par jour
        try:
            chart_data = data.get('comments_per_day', {})
            if chart_data.get('dates') and chart_data.get('counts'):
                # Vérification que les listes ont la même longueur
                if len(chart_data['dates']) == len(chart_data['counts']):
                    # Créer un DataFrame pour Plotly
                    df_plot = pd.DataFrame({
                        'Date': chart_data['dates'],
                        'Commentaires': chart_data['counts']
                    })
                    
                    fig = px.bar(
                        df_plot,
                        x='Date',
                        y='Commentaires',
                        title='Nombre de commentaires par jour',
                        labels={'Date': 'Date', 'Commentaires': 'Nombre de commentaires'}
                    )
                    fig.update_xaxis(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Données de graphique incohérentes")
            else:
                st.info("Pas assez de données pour le graphique temporel")
                
        except Exception as e:
            st.warning(f"Impossible d'afficher le graphique: {e}")
        
        return True
        
    except Exception as e:
        st.error(f"Erreur affichage résultats: {e}")
        return False

def perform_analysis(url):
    """Effectue l'analyse avec gestion d'erreurs robuste"""
    try:
        # Validation URL
        extract = Extraction(video_url=url)
        videoid = extract.url2id()
        st.session_state['videoid'] = videoid
        
    except Exception as e:
        st.error(f"URL YouTube invalide: {e}")
        return False
    
    try:
        # Connexion base
        client = Load().data_base_connexion()
        st.session_state['client_mdb'] = client
        
        # Chercher si la vidéo existe
        db_name = get_existing_db(client, videoid)
        data_exists = db_name is not None
        
        if not data_exists:
            st.info("Vidéo non trouvée, lancement de l'extraction...")
            with st.spinner("Extraction en cours..."):
                try:
                    channel_id = main_etl(url, with_channel_id=True)
                    if channel_id:
                        db_name = channel_id
                        st.success("Extraction terminée!")
                    else:
                        st.error("Échec de l'extraction")
                        return False
                except Exception as e:
                    st.error(f"Erreur extraction: {e}")
                    return False
        else:
            st.success("Vidéo trouvée dans la base de données")
        
        st.session_state['data_base_name'] = db_name
        
        # Récupérer les KPI
        video_title, sync_date, total_comments, most_liked_comment, most_liked_date, like_count = get_kpi(
            client, db_name, videoid
        )
        
        # Préparer les données du wordcloud
        wordcloud_text = prepare_wordcloud_data(client, db_name, videoid)
        
        # Préparer les données du graphique
        chart_data = prepare_chart_data(client, db_name, videoid)
        
        # Stocker toutes les données
        st.session_state.video_data = {
            'url': url,
            'video_title': video_title,
            'sync_date': sync_date,
            'total_comments': total_comments,
            'most_liked_comment': most_liked_comment,
            'most_liked_date': most_liked_date,
            'like_count': like_count,
            'wordcloud_text': wordcloud_text,
            'comments_per_day': chart_data
        }
        
        st.session_state.analysis_done = True
        Load().data_base_deconnexion(client)
        return True
        
    except Exception as e:
        st.error(f"Erreur lors de l'analyse: {e}")
        if 'client_mdb' in st.session_state and st.session_state['client_mdb']:
            Load().data_base_deconnexion(st.session_state['client_mdb'])
        return False

def main():
    """Fonction principale avec gestion d'erreurs"""
    try:
        initialize_session_state()
        
        st.title("YOU REVIEW")
        st.subheader("L'Analyse automatisée de vos commentaires YouTube", divider="gray")
        
        # Interface utilisateur
        url = st.text_input(
            "URL de la vidéo YouTube", 
            value=st.session_state.url_input, 
            placeholder="https://www.youtube.com/watch?v=..."
        )
        st.session_state['url_input'] = url
        
        # Boutons
        col1, col2 = st.columns([1, 1])
        
        with col1:
            analyze_button = st.button("🔍 Lancer l'analyse", type="primary")
        
        with col2:
            if st.session_state.analysis_done:
                if st.button("🗑️ Effacer les résultats"):
                    st.session_state.analysis_done = False
                    st.session_state.video_data = {}
                    st.session_state.url_input = ""
                    st.rerun()
        
        # Traitement
        if analyze_button:
            if not url.strip():
                st.error("Veuillez entrer une URL YouTube valide.")
                return
            
            if perform_analysis(url):
                st.rerun()
        
        # Affichage des résultats
        if st.session_state.analysis_done:
            display_analysis_results()
        
    except Exception as e:
        st.error(f"Erreur critique dans l'application: {e}")
        # Reset en cas d'erreur critique
        st.session_state.analysis_done = False
        st.session_state.video_data = {}

if __name__ == "__main__":
    main()