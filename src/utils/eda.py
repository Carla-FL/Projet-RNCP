import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
import matplotlib.pyplot as plt
import pandas as pd
from src.utils.load import Load
from src.utils.extraction import Extraction

def eda(url):
    """_summary_

    Args:
        url (_type_): _description_
    """
    client = Load.data_base_connexion()
    # Accès à la base et la collection
    db = client[Extraction(url).channel_id]
    collection = db[Extraction(video_url=url).video_id]

    # Chargement des documents dans un DataFrame
    data = list(collection.find())
    df = pd.DataFrame(data)
    # afficher les graphiques en se connectant à la base de données MongoDB
    # le nombre de commentaires par jour
    df['date'] = df['publishedAt'].dt.date
    df['date'].value_counts().sort_index().plot(kind='bar', figsize=(12, 6))
    plt.title('Nombre de commentaires par jour')
    plt.xlabel('Date')
    plt.ylabel('Nombre de commentaires')
    plt.xticks(rotation=45)
    plt.tight_layout()
