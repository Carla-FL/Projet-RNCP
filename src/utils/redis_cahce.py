import redis
import pickle
import hashlib
import json
import streamlit as st
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import os

@st.cache_resource
def get_redis_client():
    """Initialise la connexion Redis avec cache Streamlit"""
    try:
        # Pour développement local
        client = redis.Redis(
            host='localhost', 
            port=6379, 
            db=0, 
            decode_responses=False
        )
    # try:
    #     # Pour Docker
    #     client = redis.Redis(
    #         host=os.getenv('REDIS_HOST', 'redis'), 
    #         port=int(os.getenv('REDIS_PORT', 6379)), 
    #         db=0, 
    #         decode_responses=False
    #     )
        
        # Pour production avec Redis Cloud (gratuit)
        # client = redis.from_url("redis://default:password@host:port")
        
        client.ping()
        st.success("Redis connecté")
        return client
    except redis.ConnectionError:
        st.warning("Redis non disponible - fonctionnement sans cache")
        return None

class TopicModelingCache:
    """Cache Redis pour les résultats de topic modeling"""
    
    def __init__(self, redis_client=None, ttl_hours: int = 24):
        self.redis = redis_client if redis_client else get_redis_client()
        self.ttl = ttl_hours * 3600
    
    def _generate_cache_key(self, videoid: str, sentiments: list, data_hash: str) -> str:
        """Génère une clé unique pour le cache"""
        sentiments_str = "-".join(sorted(sentiments))
        return f"topic_model:{videoid}:{sentiments_str}:{data_hash}"
    
    def _calculate_data_hash(self, df: pd.DataFrame) -> str:
        """Calcule un hash des données pour détecter les changements"""
        # Hash basé sur les commentaires et sentiments
        data_string = f"{len(df)}_{df['comment'].iloc[0] if len(df) > 0 else ''}_{df['sentiment'].iloc[0] if len(df) > 0 else ''}"
        return hashlib.md5(data_string.encode()).hexdigest()[:8]
    
    def get_cached_results(self, videoid: str, sentiments: list, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Récupère les résultats depuis le cache"""
        if not self.redis:
            return None
            
        data_hash = self._calculate_data_hash(df)
        cache_key = self._generate_cache_key(videoid, sentiments, data_hash)
        
        try:
            cached_data = self.redis.get(cache_key)
            if cached_data:
                result = pickle.loads(cached_data)
                st.success("Résultats récupérés depuis le cache !")
                return result['dataframe']
            return None
        except Exception as e:
            st.warning(f"Erreur lecture cache: {e}")
            return None
    
    def cache_results(self, videoid: str, sentiments: list, df: pd.DataFrame, results_df: pd.DataFrame) -> bool:
        """Sauvegarde les résultats dans le cache"""
        if not self.redis:
            return False
            
        data_hash = self._calculate_data_hash(df)
        cache_key = self._generate_cache_key(videoid, sentiments, data_hash)
        
        try:
            cache_data = {
                'dataframe': results_df,
                'metadata': {
                    'videoid': videoid,
                    'sentiments': sentiments,
                    'num_comments': len(df),
                    'cached_at': datetime.now().isoformat(),
                    'num_topics': len(results_df['topic_id'].unique()) if 'topic_id' in results_df.columns else 0
                }
            }
            
            serialized_data = pickle.dumps(cache_data)
            success = self.redis.setex(cache_key, self.ttl, serialized_data)
            
            if success:
                st.info("Résultats sauvegardés en cache")
                return True
            return False
        except Exception as e:
            st.warning(f"Erreur sauvegarde cache: {e}")
            return False