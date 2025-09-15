import pandas as pd
import os
import pickle
import logging
import numpy as np
from typing import Optional, Dict, Any, List
from transformers import pipeline
from huggingface_hub import hf_hub_download
from prefect import task, get_run_logger

# Configuration des modèles
MODEL_CONFIG = {
    "finetuned": {
        "repo_id": "Carlito-25/sentiment-model-finetuned",
        "type": "transformers",
        "priority": 1
    },
    "original": {
        "repo_id": "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
        "type": "transformers", 
        "priority": 2
    },
    "logistic": {
        "repo_id": "Carlito-25/sentiment-model-logistic",
        "type": "sklearn",
        "priority": 3
    }
}

class SentimentModelManager:
    """Gestionnaire de modèles avec fallback automatique"""
    
    def __init__(self):
        self.current_model = None
        self.current_model_name = None
        self.model_priority = os.getenv("MODEL_PRIORITY", "finetuned,original,logistic").split(",")
        self.logger = self._get_logger()
        
    def _get_logger(self):
        """Récupère le logger approprié (Prefect ou standard)"""
        try:
            return get_run_logger()
        except:
            logging.basicConfig(level=logging.INFO)
            return logging.getLogger(__name__)
    
    def _load_transformers_model(self, repo_id: str, model_name: str) -> Optional[Any]:
        """Charge un modèle transformers depuis Hugging Face"""
        try:
            self.logger.info(f"🔄 Chargement du modèle transformers: {model_name}")
            
            model = pipeline(
                "sentiment-analysis",
                model=repo_id,
                device=-1,  # CPU only pour compatibilité
                truncation=True,
                max_length=514,
                return_all_scores=False
            )
            
            # Test rapide du modèle
            test_result = model("Test sentiment")
            self.logger.info(f"✅ Modèle {model_name} chargé et testé avec succès")
            return model
            
        except Exception as e:
            self.logger.warning(f"❌ Échec du chargement de {model_name}: {str(e)}")
            return None
    
    def _load_sklearn_model(self, repo_id: str, model_name: str) -> Optional[Any]:
        """Charge un modèle sklearn depuis Hugging Face"""
        try:
            self.logger.info(f"🔄 Chargement du modèle sklearn: {model_name}")
            
            # Télécharger le fichier pickle
            model_path = hf_hub_download(
                repo_id=repo_id,
                filename="model.pkl",
                cache_dir="./.model_cache"
            )
            
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            
            self.logger.info(f"✅ Modèle {model_name} chargé avec succès")
            return model
            
        except Exception as e:
            self.logger.warning(f"❌ Échec du chargement de {model_name}: {str(e)}")
            return None
    
    def _predict_transformers(self, model: Any, text: str) -> Dict[str, Any]:
        """Prédiction avec un modèle transformers"""
        try:
            result = model(text)[0]
            return {
                'label': result['label'],
                'score': result['score'],
                'model_used': self.current_model_name
            }
        except Exception as e:
            self.logger.error(f"Erreur prédiction transformers: {e}")
            raise
    
    def _predict_sklearn(self, model: Any, text_features: np.ndarray) -> Dict[str, Any]:
        """Prédiction avec un modèle sklearn"""
        try:
            if isinstance(text_features, list):
                text_features = np.array(text_features).reshape(1, -1)
            
            prediction = model.predict(text_features)[0]
            probabilities = model.predict_proba(text_features)[0]
            
            # Mapper les prédictions sklearn vers le format transformers
            label_mapping = {0: 'NEGATIVE', 1: 'NEUTRAL', 2: 'POSITIVE'}
            
            return {
                'label': label_mapping.get(prediction, 'NEUTRAL'),
                'score': float(np.max(probabilities)),
                'model_used': self.current_model_name
            }
        except Exception as e:
            self.logger.error(f"Erreur prédiction sklearn: {e}")
            raise
    
    def load_best_available_model(self) -> bool:
        """Charge le meilleur modèle disponible selon la priorité"""
        self.logger.info("🚀 Chargement du meilleur modèle disponible...")
        
        for model_name in self.model_priority:
            if model_name not in MODEL_CONFIG:
                self.logger.warning(f"⚠️ Modèle {model_name} non configuré")
                continue
                
            config = MODEL_CONFIG[model_name]
            
            if config["type"] == "transformers":
                model = self._load_transformers_model(config["repo_id"], model_name)
            elif config["type"] == "sklearn":
                model = self._load_sklearn_model(config["repo_id"], model_name)
            else:
                self.logger.warning(f"⚠️ Type de modèle {config['type']} non supporté")
                continue
            
            if model is not None:
                self.current_model = model
                self.current_model_name = model_name
                self.current_model_type = config["type"]
                self.logger.info(f"🎯 Modèle actif: {model_name}")
                return True
        
        self.logger.error("❌ Aucun modèle n'a pu être chargé!")
        return False
    
    def predict(self, text: str, text_features: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """Prédiction avec le modèle actuel"""
        if self.current_model is None:
            raise RuntimeError("Aucun modèle n'est chargé. Appelez load_best_available_model() d'abord.")
        
        if self.current_model_type == "transformers":
            return self._predict_transformers(self.current_model, text)
        elif self.current_model_type == "sklearn":
            if text_features is None:
                raise ValueError("text_features requis pour les modèles sklearn")
            return self._predict_sklearn(self.current_model, text_features)
        else:
            raise RuntimeError(f"Type de modèle non supporté: {self.current_model_type}")
    
    def get_model_info(self) -> Dict[str, str]:
        """Informations sur le modèle actuel"""
        return {
            "model_name": self.current_model_name or "Aucun",
            "model_type": getattr(self, 'current_model_type', "Aucun"),
            "repo_id": MODEL_CONFIG.get(self.current_model_name, {}).get("repo_id", "Aucun")
        }

# Instance globale du gestionnaire
model_manager = SentimentModelManager()

@task(name='sentiment_model_creation', description="Initialisation du gestionnaire de modèles d'analyse des sentiments")
def get_sentiment_model() -> SentimentModelManager:
    """
    Remplace ton ancienne fonction get_sentiment_model()
    Charge automatiquement le meilleur modèle disponible
    """
    if model_manager.current_model is None:
        success = model_manager.load_best_available_model()
        if not success:
            raise RuntimeError("Impossible de charger un modèle d'analyse de sentiment")
    
    return model_manager

@task(name='sentiment_analyse', description="Analyse des sentiments des commentaires avec fallback automatique")
def get_sentiment(df, model=None, text='comment'):
    """
    Version améliorée de ta fonction get_sentiment()
    Compatible avec ton code existant
    """
    logger = get_run_logger()
    
    # Initialiser le gestionnaire si nécessaire
    sentiment_model = get_sentiment_model()
    
    logger.info(f"📊 Analyse de sentiment avec le modèle: {sentiment_model.current_model_name}")
    
    def analyze_single_comment(comment_text):
        try:
            if sentiment_model.current_model_type == "transformers":
                result = sentiment_model.predict(comment_text)
                return result['label'], result['score']
            else:
                # Pour sklearn, il faudrait les features - fallback vers transformers
                logger.warning("Modèle sklearn détecté mais features manquantes, tentative de fallback")
                # Essayer de charger un modèle transformers
                for model_name in ["finetuned", "original"]:
                    if model_name in MODEL_CONFIG:
                        config = MODEL_CONFIG[model_name]
                        if config["type"] == "transformers":
                            backup_model = sentiment_model._load_transformers_model(config["repo_id"], model_name)
                            if backup_model:
                                result = backup_model(comment_text)[0]
                                return result['label'], result['score']
                
                return 'NEUTRAL', 0.5  # Fallback par défaut
        except Exception as e:
            logger.warning(f"Erreur analyse sentiment pour '{comment_text[:50]}...': {e}")
            return 'NEUTRAL', 0.5
    
    # Appliquer l'analyse sur tout le DataFrame
    df[['sentiment', 'sentiment_score']] = df[text].apply(lambda x: pd.Series(analyze_single_comment(str(x))))
    # Statistiques
    sentiment_counts = df['sentiment'].value_counts()
    logger.info(f"📈 Résultats analyse: {dict(sentiment_counts)}")
    
    return df

# # Fonction utilitaire pour tester le système
# def test_model_system():
#     """Test du système de modèles"""
#     print("🧪 Test du système de modèles...")
    
#     try:
#         manager = get_sentiment_model()
#         info = manager.get_model_info()
#         print(f"✅ Modèle chargé: {info}")
        
#         # Test de prédiction
#         test_texts = [
#             "J'adore cette vidéo, elle est fantastique!",
#             "Cette vidéo est vraiment nulle.",
#             "C'est une vidéo normale, sans plus."
#         ]
        
#         for text in test_texts:
#             result = manager.predict(text)
#             print(f"📝 '{text}' → {result['label']} ({result['score']:.3f})")
            
#     except Exception as e:
#         print(f"❌ Erreur test: {e}")

# if __name__ == "__main__":
#     test_model_system()
