
""" ________________________________________________________________   Library   ________________________________________________________________"""
from pymongo import MongoClient
import json
import pandas as pd
import os
from dotenv import load_dotenv
from prefect import flow, task
from prefect import get_run_logger
from pymongo import UpdateOne, DeleteMany, InsertOne
# from .transformation import main_transformation
# from .extraction import Extraction
load_dotenv()
""" ________________________________________________________________  Fonctions  ________________________________________________________________"""

class Load:
    def __init__(self, maj=False):
        """
        Initialisation de la classe Load.
        """
        self.maj = maj  # Indique si c'est une mise à jour ou un chargement initial
        self.username = str(os.getenv('MONGO_USERNAME'))
        self.password = str(os.getenv('MONGO_PASSWORD'))
        self.host = str(os.getenv('MONGO_HOST'))
        self.port = str(os.getenv('MONGO_PORT'))
        self.authSource = str(os.getenv('MONGO_AUTH_SOURCE'))
        

    @task(name='authentification_task', description="Tâche d'authentification pour la connexion à MongoDB")
    def authentification(self):
        """
        Fonction pour récupérer les informations d'authentification depuis un fichier de configuration.
        """
        try:
            #with open('/Users/carla/Desktop/GitHub/Projet-RNCP/auth.json', 'r') as file:
                #lines = json.load(file)
            username =  self.username  # Encodage du nom d'utilisateur lines['username'] #
            password =  self.password  # Encodage du mot de passe lines['password'] #
            host =  self.host  # Adresse du serveur MongoDB lines['host'] #
            port = self.port  # Port du serveur MongoDB lines['port'] # 
            authSource =  self.authSource  # Base de données d'authentification lines['authSource'] #
            return username, password, host, port, authSource
        except FileNotFoundError:
            raise FileNotFoundError("Le fichier de configuration est introuvable.")

    @task(name='data_base_connexion_task', description="Tâche de connexion à la base de données MongoDB", retries=3, retry_delay_seconds=5)
    def data_base_connexion(self):
        """
        Fonction pour se connecter à la base de données MongoDB.
        """

        username, password, host, port, auth_db = self.authentification()
        # Création de l'URI de connexion MongoDB
        uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_db}"
        print(f"Connecting to MongoDB at {uri}")
        client = MongoClient(uri)
        # Vérification de la connexion
        try:
            client.admin.command('ping')  # Ping pour vérifier la connexion
            print("Connexion à MongoDB réussie.")
            return client
        except Exception as e:
            raise RuntimeError(f"Erreur de connexion à MongoDB : {e}")
        
        
    def data_base_deconnexion(self, client):
        """
        Fonction pour fermer la connexion à la base de données MongoDB.
        Args:
            client (MongoClient): Instance de connexion à MongoDB.
        Returns:
            None
        """
        client.close()
        print("Connexion à MongoDB fermée.")


    # @task(name='check_exisitng_data_task', description="Tâche de vérification de l'existence des données dans MongoDB")
    def check_exisitng_data(self, db_name:str, collection_name:str):
        """
        Vérifie si une collection existe déjà dans la base de données.
        Args:
            client (MongoClient): Instance de connexion à MongoDB.
            db_name (str): Nom de la base de données / id de la chaîne.
            collection_name (str): Nom de la collection à vérifier (identifiant de la vidéo).
        Returns:
            None si la collection n'existe pas.
            une liste d'IDs si la collection existe.
        """
        client = self.data_base_connexion()
        # Lister les bases de données
        try:
            # Lister les bases de données
            db_list = client.list_database_names()
        
            if db_name not in db_list:
                print(f"Il n'existe pas encore de données pour cette chaîne : {db_name}.")
                self.data_base_deconnexion(client)
                return None
            # Lister les collections dans la base de données spécifiée
            collection_list = client[db_name].list_collection_names()

            if collection_name not in collection_list:
                print(f"La collection '{collection_name}' n'existe pas dans la base de données '{db_name}'.")
                self.data_base_deconnexion(client)
                return None
            print(f"La collection '{collection_name}' existe déjà dans la base de données '{db_name}'.")

            # Récupérer les IDs uniques de la collection
            ids = client[db_name][collection_name].distinct("id")
            
            # Récupérer les colonnes/champs disponibles
            # Méthode 1: Examiner un document échantillon pour obtenir les champs
            # sample_doc = client[db_name][collection_name].find_one()
            # columns = list(sample_doc.keys()) if sample_doc else []

            return ids#, columns
        
        except Exception as e:
            print(f"Erreur lors de la vérification des données : {e}")
            return None
        
        finally:
            self.data_base_deconnexion(client)

        
    @flow(name='load_data', description="Chargement des données dans MongoDB")
    def load(self, df, video_id:str, channel_id:str):
        logger = get_run_logger()

        client = self.data_base_connexion()
        # création de la base
        db = client[channel_id] # idenentifiant de la chaine
        # création de la table dans la base
        collection = db[video_id] # indentifiant de la video

        if self.maj == True:
            # # logger.info("Mise à jour des données dans MongoDB...")
            comments_data = df.to_dict('records')
            # operations = [UpdateOne(
            #         {"id": comment["id"]},
            #         {"$set": comment},
            #         upsert=True)
            #     for comment in comments_data
            # ]

            # if operations:
            #     collection.bulk_write(operations)
            # Supprime tous les documents existants
            db[video_id].delete_many({})
            # Insère tous les nouveaux documents
            db[video_id].insert_many(comments_data)


            self.data_base_deconnexion(client)

        else :
            # Insertion
            # logger.info("Insertion des données dans MongoDB...")
            collection.insert_many(df.to_dict(orient="records"))
            logger.info("Données insérées avec succès dans MongoDB.")
            self.data_base_deconnexion(client)

        # Mise à jour de la collection


""" ________________________________________________________________  Main  ________________________________________________________________"""
# if __name__ == "__main__":
#     Load.load()


# def check_existing_data(self, db_name:str, collection_name:str):

#     client = self.data_base_connexion()
    
#     try:
#         # Lister les bases de données
#         db_list = client.list_database_names()
        
#         if db_name not in db_list:
#             print(f"Il n'existe pas encore de données pour cette chaîne : {db_name}.")
#             return None
        
#         # Lister les collections dans la base de données spécifiée
#         collection_list = client[db_name].list_collection_names()
        
#         if collection_name not in collection_list:
#             print(f"La collection '{collection_name}' n'existe pas dans la base de données '{db_name}'.")
#             return None
        
#         print(f"La collection '{collection_name}' existe déjà dans la base de données '{db_name}'.")
        
#         # Récupérer la collection
#         collection = client[db_name][collection_name]
        
#         # Récupérer les IDs uniques
#         ids = collection.distinct("id")
        
#         # Récupérer les colonnes/champs disponibles
#         # Méthode 1: Examiner un document échantillon pour obtenir les champs
#         sample_doc = collection.find_one()
#         columns = list(sample_doc.keys()) if sample_doc else []
        
#         # Méthode alternative: Examiner plusieurs documents pour avoir tous les champs possibles
#         # (utile si tous les documents n'ont pas exactement les mêmes champs)
#         """
#         all_fields = set()
#         for doc in collection.find().limit(100):  # Limite pour éviter de charger trop de données
#             all_fields.update(doc.keys())
#         columns = list(all_fields)
#         """
        


#     finally:
#         self.data_base_deconnexion(client)