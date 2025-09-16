# import sys
# sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
# import sys
import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pathlib
rootdir = pathlib.Path(__file__).parent.parent.parent.parent.parent.resolve()
import sys
sys.path.append(str(rootdir))
import streamlit as st
from pages import page1, page2
import streamlit as st
from streamlit_config import initialize_streamlit_app, get_model_manager, add_debug_sidebar
import pandas as pd

# Initialisation
logger = initialize_streamlit_app()

# Sidebar de debug (optionnel)
add_debug_sidebar()

st.set_page_config(layout="wide",
                page_title="YOU REVIEW ANALYSER")

# st.logo(os.path.join(str(rootdir), "ressources", "incon.png"), size="large")
st.logo("./ressources/incon.png", size="large",)
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