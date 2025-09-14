# import sys
# sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
# import sys
import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pathlib
rootdir = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
import sys
sys.path.append(str(rootdir))
import streamlit as st
from pages import page1, page2
import streamlit as st
from streamlit_config import initialize_streamlit_app, get_model_manager, add_debug_sidebar

# Initialisation
logger = initialize_streamlit_app()

# Sidebar de debug (optionnel)
add_debug_sidebar()

st.set_page_config(layout="wide",
                page_title="YOU REVIEW ANALYSER")

st.logo(os.path.join(str(rootdir), "ressources", "incon.png", size="large"))
# logo_path = os.path.join(os.getcwd(), "ressources", "icon.png")  # correction du nom
# if os.path.exists(logo_path):
#     st.logo(logo_path, size="large")

# Initialisation des variables de session globales
def init_global_session_state():
    """Initialise les variables globales de session"""
    if 'current_analysis' not in st.session_state:
        st.session_state.current_analysis = None
    if 'url_input' not in st.session_state:
        st.session_state.url_input = ""
    if 'videoid' not in st.session_state:
        st.session_state.videoid = None
    if 'data_base_name' not in st.session_state:
        st.session_state.data_base_name = None
    if 'client_mdb' not in st.session_state:
        st.session_state.client_mdb = None

# Appeler l'initialisation
init_global_session_state()


pages = {
    " 1 - Accueil": page1.main,
    " 2 - Analyse de sentiment": page2.main
}

st.sidebar.title('Navigation')
p = st.sidebar.radio('Aller à  ', list(pages.keys()))

st.sidebar.markdown("-------------------")


# Bouton pour effacer toutes les données
if st.sidebar.button("🗑️ Effacer toutes les données"):
    # Effacer toutes les variables de session liées à l'analyse
    keys_to_clear = ['analysis_done', 'video_data', 'current_analysis', 'url_input', 
                    'videoid', 'data_base_name', 'client_mdb']
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    st.sidebar.success("Données effacées!")
    st.rerun()

pages[p]()

# def eda(url):
#     """_summary_

#     Args:
#         url (_type_): _description_
#     """
#     client = Load.data_base_connexion()
#     # Accès à la base et la collection
#     db = client[Extraction(url).channel_id]
#     collection = db[Extraction(video_url=url).video_id]

#     # Chargement des documents dans un DataFrame
#     data = list(collection.find())
#     df = pd.DataFrame(data)
#     # afficher les graphiques en se connectant à la base de données MongoDB
#     # le nombre de commentaires par jour
#     df['date'] = df['publishedAt'].dt.date
#     df['date'].value_counts().sort_index().plot(kind='bar', figsize=(12, 6))
#     plt.title('Nombre de commentaires par jour')
#     plt.xlabel('Date')
#     plt.ylabel('Nombre de commentaires')
#     plt.xticks(rotation=45)
#     plt.tight_layout()



#     eda(url)



# # mentions légales et CGU et politique de confidentialité

# # mettre une barre latérale vers toutes les pages de l'application
# # Page d'ajout d'expressions
# FICHIER_EXPRESSIONS = "extra_expressions.txt"

# # Chargement du dictionnaire existant
# if os.path.exists(FICHIER_EXPRESSIONS):
#     with open(FICHIER_EXPRESSIONS, "r", encoding="utf-8") as f:
#         expressions = json.load(f)
# else:
#     expressions = {}

# st.title("📝 Ajouter des expressions normalisées")

# # Interface déroulante
# if st.button("➕ Ajouter des expressions"):
#     st.session_state["ajout_visible"] = not st.session_state.get("ajout_visible", False)

# if st.session_state.get("ajout_visible", False):
#     st.markdown("### Nouvelle(s) expression(s)")
    
#     if "form_inputs" not in st.session_state:
#         st.session_state.form_inputs = [{"text": "", "cleaned": ""}]

#     # Afficher les champs dynamiques
#     for i, entry in enumerate(st.session_state.form_inputs):
#         st.session_state.form_inputs[i]["text"] = st.text_input(f"Expression {i+1}", value=entry["text"], key=f"expr_{i}")
#         st.session_state.form_inputs[i]["cleaned"] = st.text_input(f"Version normalisée {i+1}", value=entry["cleaned"], key=f"clean_{i}")
    
#     # Bouton pour ajouter un champ
#     if st.button("➕ Ajouter un champ"):
#         st.session_state.form_inputs.append({"text": "", "cleaned": ""})

#     # Validation
#     if st.button("✅ Enregistrer"):
#         new_entries = {}
#         for entry in st.session_state.form_inputs:
#             key = entry["text"].strip().lower()
#             val = entry["cleaned"].strip().lower()
#             if key and val:
#                 new_entries[key] = val
#         # Mise à jour du fichier
#         expressions.update(new_entries)
#         with open(FICHIER_EXPRESSIONS, "w", encoding="utf-8") as f:
#             json.dump(expressions, f, ensure_ascii=False, indent=4)
        
#         st.success(f"{len(new_entries)} expression(s) ajoutée(s) au fichier.")
#         st.session_state.form_inputs = []  # reset après ajout

# # Page d'ajouts de stopwords