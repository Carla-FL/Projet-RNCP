""" ________________________________________________________________   Library   ________________________________________________________________"""

import spacy
import re
import json
import pandas as pd
import numpy as np
import nltk
import pickle
from nltk.stem.snowball import SnowballStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
import string
from nltk.probability import FreqDist
import matplotlib.pyplot as plt
from unidecode import unidecode
from gensim.models import Word2Vec
from gensim.utils import simple_preprocess
from prefect import task, flow, get_run_logger
from transformers import pipeline
# Nouveau import en haut du fichier
from model_manager import get_sentiment_model, get_sentiment

nlp = spacy.load("fr_core_news_sm")
stopwords = list(nlp.Defaults.stop_words)
punctuation = list(string.punctuation)
s_stemmer = SnowballStemmer("french")

""" ________________________________________________________________  nettoyage ________________________________________________________________"""
# ÉCRIRE UN FICHIER AVEC LES EXTRAS STOPWORDS ET LES EXPRESSIONS que l'utilisateur pourra agrémenter : 
# appel de la fonction s'il y a un input, ouverture et écriture des lignes supplémentaires, sauvegarde du document pour utilisation
## @task(name='expressions_frequentes', cache_policy=NO_CACHE, description="Remplace les expressions fréquentes par leur version normalisée")
def expressions_frequentes(text, path='extra_expressions.txt'):
    with open(path, 'r') as file:
        expressions = json.load(file)
    for key, value in expressions.items():
        text = re.sub(key, value, text.lower())
    return text

## @task(name='reduire_repetitions', cache_policy=NO_CACHE, description="Réduit les répétitions de lettres dans les mots")
def reduire_repetitions(mot):
    """
    nettoyage des répétitions de lettres
    On remplace les répétitions de lettres par une seule occurrence
    Ex: "loooove" devient "love"
    """
    return re.sub(r'(.)\1{2,}', r'\1', mot)

# # @task(name='preprocessing_task', description="Tâche de prétraitement des données textuelles")
def preprocessing(text, join=True, methode='lemma', extra_stopwords : list = None, extra_punctuation : list = None, extra_pattern : re.Pattern=None, path='extra_expressions.txt'):
    if not isinstance(text, str):
        raise ValueError(f"Expected a string, but got {type(text)} : {text}")
    extra_stopwords = extra_stopwords or []
    extra_punctuation = extra_punctuation or []
    nondesiredtokens = set(punctuation + stopwords + extra_punctuation + extra_stopwords)
    
    text = expressions_frequentes(text, path=path)  # Remplacer les expressions fréquentes par leur version normalisée

    # pattern = re.compile(r"(http://\S+|# @\S+|.*\d.*|.*\#.*)") 
    pattern = re.compile(r'http.+')

    # Tokenisation et nettoyage en une seule passe
    if methode == 'lemma':
        tokens = [unidecode(pattern.sub("", token.lemma_.lower()))  for token in nlp(text) if not token.is_stop and not token.is_digit and token.text.lower() not in nondesiredtokens and len(token) > 3]
    elif methode == 'stem':
        tokens = [unidecode(pattern.sub("", s_stemmer.stem(token.text.lower())))  for token in nlp(text) if not token.is_stop and not token.is_digit and token.text.lower() not in nondesiredtokens and len(token) > 3]
    
    # Filtrer les tokens vides après suppression des liens/mentions
    cleaned_tokens = [reduire_repetitions(token) for token in tokens if len(token)>3]
    # re.sub(r'\-{1,}|\\{1,}|\/{1,}|\_{1,}|~{1,}|\*{1,}|\?{1,}|\={1,}|\:{1,}|\){1,}|\({1,}', '',
    
    return " ".join(cleaned_tokens) if join else cleaned_tokens


""" ____________________________________________________________  Vectorisation  ______________________________________________________________"""


@task(name='w2vec_model_creation', description="Création du modèle Word2Vec à partir des données prétraitées")
def make_w2vec_model(df:pd.DataFrame, text='comment_clean_lem'):
    """
    Obtenir le vecteur d'un mot spécifique
    """
    try:
        corpus = df[text].dropna().tolist()
        tokenized_corpus = [simple_preprocess(sent) for sent in corpus]
        model_w2v = Word2Vec(sentences=tokenized_corpus, vector_size=100, window=5, min_count=1, sg=1)
        return model_w2v
    except KeyError:
        raise ValueError("Error : le modèle Word2Vec n'a pas été entraîné correctement.")
    
def get_sentence_vector(text, model):
    words = simple_preprocess(text)
    word_vectors = [model.wv[word] for word in words if word in model.wv]
    if len(word_vectors) > 0:
        return np.mean(word_vectors, axis=0)  # moyenne des vecteurs
    else:
        return np.zeros(model.vector_size)
    
# # @task(name='w2vec_vector_creation', description="Extraction des vecteurs de phrases à partir du modèle Word2Vec")
def get_w2vec_vector(df, text='comment_clean_lem'):
    """
    Obtenir le vecteur d'une phrase en utilisant le modèle Word2Vec
    """
    try:
        model = make_w2vec_model(df, text=text)
        df['w2vec_vector_np'] = df[text].apply(lambda x: get_sentence_vector(x, model))
        df['w2vec_vector'] = df['w2vec_vector_np'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
        df.drop(columns=['w2vec_vector_np'], inplace=True)  # Optionnel : supprimer la colonne intermédiaire
    
        return df
    except ValueError as e:
        raise ValueError(f"Erreur lors de la création du modèle Word2Vec : {e}")

@task(name='tfidf_vector_creation', description="Extraction des vecteurs TF-IDF à partir des données prétraitées")  
def get_tfidf_vector(df, text='comment_clean_lem'):
    """
    Obtenir le vecteur TF-IDF d'une phrase
    """
    if not all(isinstance(x, str) for x in df[text].dropna()):
        raise ValueError(f"La colonne '{text}' ne contient pas que des chaînes de caractères")
    
    vectorizer = TfidfVectorizer()
    corpus = df[text].dropna().tolist()
    tfidf_matrix = vectorizer.fit_transform(corpus)
    df['tfidf_vector_np'] = list(tfidf_matrix.toarray())
    df['tfidf_vector'] = df['tfidf_vector_np'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    df.drop(columns=['tfidf_vector_np'], inplace=True)  # Optionnel : supprimer la colonne intermédiaire
    return df

""" ________________________________________________________________    analyse  ________________________________________________________________"""
# @task(name='sentiment_model_creation', description="Création du modèle d'analyse des sentiments")
# def get_sentiment_model(path=None): # '/Users/carla/Desktop/GitHub/Projet-RNCP/src/utils/bestmodel.pkl'
#     # with open(path, 'rb') as fichier_modele:
#     #     model = pickle.load(fichier_modele)
#     model = pipeline("sentiment-analysis", model="cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual", truncation=True,max_length=512)
#     return model

# @task(name='sentiment_analyse', description="Analyse des sentiments des commentaires")
# def get_sentiment(df, model, text='comment'):
#     model = get_sentiment_model() 
#     # df['sentiment'] = df[text].apply(lambda x: model.predict([x])[0])
#     df['sentiment']= df[text].apply(lambda x: model(x)[0].get('label'))
#     df['sentiment_score'] = df[text].apply(lambda x: model(x)[0].get('score'))
#     return df


""" ________________________________________________________________  Main  ________________________________________________________________"""
@flow(name='etl_transformation_pipeline', description="Pipeline de transformation des données pour l'ETL")
def main_transformation(df, comment = 'comment', path='extra_expressions.txt'):
    logger= get_run_logger()
    try:
        # df, video_id, channel_id = Extraction.main_extraction()  # Assuming main() returns a DataFrame with a 'comment' column
        df = df.dropna(subset=[comment])  # Drop rows where 'comment' is NaN
        df['tokens_clean_lem'] = df[comment].apply(lambda x: preprocessing(x, join=False, path=path))
        df['comment_clean_lem'] = df[comment].astype(str).apply(lambda x: preprocessing(x, join=True, path=path))
        logger.info("Nettoyage des données terminée avec succès.")
        
        # Vectorisation
        df = get_w2vec_vector(df, text='comment_clean_lem')
        logger.info("Vectorisation w2vec des données terminée avec succès.")

        df = get_tfidf_vector(df, text='comment_clean_lem')
        logger.info("Vectorisation tfidf des données terminée avec succès.")

        # Sentiment Analysis
        df = get_sentiment(df,model=None, text='comment')  # Remplacez 'model' par votre modèle de sentiment
        return df # video_id, channel_id
    except Exception as e:
        logger.error(f"Erreur lors de la transformation des données : {e}")
        raise e

# if __name__ == "__main__":
    # df = pd.read_csv('database.csv',parse_dates=['publishedAt','extractedAt'])
    # df = main_extraction()  # Assuming main() returns a DataFrame with a 'comment' column
    # # pattern = re.compile(r"(http://\S+|# @\S+|.*\d.*|.*\#.*)") 
    # df['tokens_clean_lem'] = df['comment'].astype(str).apply(lambda x: preprocessing(x, join=False, extra_stopwords=['vidéo','vidéos','video','videos','.', 'jsuis', 'heyy', 'faire'], extra_punctuation=[']']))
    # df['comment_clean_lem'] = df['comment'].astype(str).apply(lambda x: preprocessing(x, join=True, extra_stopwords=['vidéo','vidéos','video','videos','.', 'jsuis', 'heyy', 'faire'], extra_punctuation=[']']))
    # df, video_id, channel_id = Extraction.main_tranformation()
    # Sauvegarder le DataFrame transformé
    # df.to_csv('transformed_comments.csv', index=False)