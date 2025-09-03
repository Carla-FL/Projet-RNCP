import sys
sys.path.append("/Users/carla/Desktop/GitHub/Projet-RNCP")
import pandas as pd
from gensim import corpora
from gensim import models
import pyLDAvis.gensim
from gensim.models import LdaModel
from gensim.corpora.dictionary import Dictionary
import matplotlib.pyplot as plt
import numpy as np
from gensim.models import CoherenceModel
import warnings
warnings.filterwarnings("ignore")
from gensim.models import LdaModel, CoherenceModel
from src.utils.redis_cahce import TopicModelingCache


class TopicModeling:
    def __init__(self, df, videoid=None, sentiments=None):
        """
        Initialisation de la classe TopicModeling.
        
        Args:
            df: DataFrame contenant les documents à analyser.
        """
        self.df = df
        self.videoid = videoid
        self.sentiments = sentiments or []
        self.cache = TopicModelingCache()
        self.corpus = [doc for doc  in df['tokens_clean_lem']]
        self.dictionary = corpora.Dictionary(self.corpus)
        self.dictionary.filter_extremes(no_below=20, no_above=0.5)
        self.corpus_vect = [self.dictionary.doc2bow(doc) for doc in self.corpus]
        self.optimal_model = None
        self.optimal_num_topics = None
        

    # def get_corpus_vect(self):
    #     return [self.dictionary.doc2bow(doc) for doc in self.corpus]
    
    # def get_dictionary(self):
    #     return corpora.Dictionary(self.corpus).filter_extremes(no_below=20, no_above=0.5)


    def compute_coherence_values(self, start, limit, step):
        coherence_values_cv = []
        coherence_values_umass = []
        coherence_values_npmi = []
        perplexity_values = []
        model_list = []

        for num_topics in range(start, limit + 1, step):
            try :

                model = LdaModel(corpus=self.corpus_vect,
                                id2word=self.dictionary,
                                alpha='auto',
                                eta='auto',
                                iterations=400,
                                num_topics=num_topics,
                                chunksize=2000,
                                passes=50,
                                eval_every=None)
            except Exception as e:
                raise ValueError(f"Erreur lors de la création du modèle pour {num_topics} topics : {e}")
            
            model_list.append(model)

            cm_cv = CoherenceModel(model=model, texts=self.corpus, corpus=self.corpus_vect, dictionary=self.dictionary, coherence='c_v')
            coherence_values_cv.append(cm_cv.get_coherence())

            # Cohérence U_MASS
            cm_umass = CoherenceModel(model=model, texts=self.corpus, corpus=self.corpus_vect, dictionary=self.dictionary, coherence='u_mass')
            coherence_values_umass.append(cm_umass.get_coherence())

            # Cohérence NPMI
            cm_npmi = CoherenceModel(model=model, texts=self.corpus, corpus=self.corpus_vect, dictionary=self.dictionary, coherence='c_npmi')
            coherence_values_npmi.append(cm_npmi.get_coherence())

            # Perplexité
            perplexity_values.append(model.log_perplexity(self.corpus_vect))

            # coherencemodel = CoherenceModel(model=model, corpus=corpus, texts=texts, dictionary=dictionary, coherence='c_v')
            # coherence_values.append(coherencemodel.get_coherence())

        return model_list, coherence_values_cv, coherence_values_umass, coherence_values_npmi, perplexity_values



    def get_best_model_topic(self):
        # Paramètres
        start = 3
        limit = 10
        step = 1


        # Calcul des valeurs de cohérence

        model_list, cv_scores, umass_scores, npmi_scores, perplexities = self.compute_coherence_values(start=start, limit=limit, step=step)

        # Affichage des résultats
        x = range(start, limit + 1, step)

        # Affichage des scores de cohérence
        for num_topics, cv, um, npmi, perp in zip(x, cv_scores, umass_scores, npmi_scores, perplexities):
            # print(f"Nombre de topics = {m}, Score de cohérence = {round(cv, 4)}")
            print(f"Topics={num_topics} | C_V={cv:.4f} | U_MASS={um:.4f} | NPMI={npmi:.4f} | Perplexité={perp:.4f}")


        # Trouver le nombre optimal de topics
        optimal_model_index = np.argmax(cv_scores)
        optimal_num_topics = x[optimal_model_index]
        print(f"Nombre optimal de topics : {optimal_num_topics} avec un score de cohérence de {cv_scores[optimal_model_index]}")
        # Affichage du modèle optimal
        optimal_model = model_list[optimal_model_index]
        return optimal_model, optimal_num_topics


    def main_topic_modeling(self):
        """
        Fonction principale pour le topic modeling.
        
        Args:
            corpus: Liste de documents (corpus).
            corpus_vect: Corpus sous forme de bag-of-words.
            dictionary: Dictionnaire Gensim.
            
        Returns:
            optimal_model: Le modèle LDA optimal.
            optimal_num_topics: Le nombre optimal de topics.
        """
        if self.videoid and self.sentiments:
            cached_results = self.cache.get_cached_results(self.videoid, self.sentiments, self.df)
            if cached_results is not None:
                return cached_results
            

        optimal_model, optimal_num_topics = self.get_best_model_topic()
        # Appliquer le modèle optimal aux documents
        all_topics = []
        dominant_topics = []
        topic_probs = []
        all_key_words = []

        for bow in self.corpus_vect:  # corpus_vect = ton corpus bag-of-words
            topic_dist = optimal_model.get_document_topics(bow)  # liste (topic_id, prob)
            all_topics.append(topic_dist)
            
            # Topic dominant
            top_topic, top_prob = max(topic_dist, key=lambda x: x[1])
            key_words = ", ".join([word for word, _ in optimal_model.show_topic(top_topic)])
            dominant_topics.append(top_topic)
            topic_probs.append(top_prob)
            all_key_words.append(key_words)

        if len(all_topics) == len(dominant_topics)== len(topic_probs)==len(all_key_words)==len(self.df):
            result_df = self.df.copy()
            result_df["topic_id"] = dominant_topics
            result_df["topic_prob"] = topic_probs
            result_df["topic_keywords"] = all_key_words

            # sauvegarde en cache
            if self.videoid and self.sentiments:
                self.cache.cache_results(self.videoid, self.sentiments, self.df, result_df)

            return result_df
        
        else:
            raise ValueError("Les longueurs des listes ne correspondent pas. Vérifiez les données d'entrée.")