#!/usr/bin/env python3
"""
Script pour uploader tes modèles vers Hugging Face Hub
Usage: python upload_models.py
"""

import os
import pickle
import json
from huggingface_hub import HfApi, create_repo, upload_folder, upload_file
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Configuration
HF_USERNAME = "Carlito-25"
HF_TOKEN = os.getenv('HF_TOKEN')  # À définir dans tes variables d'environnement

# Informations des modèles
MODELS_CONFIG = {
    "finetuned": {
        "repo_name": f"{HF_USERNAME}/sentiment-model-finetuned",
        "local_path": "./notebooks/mon_modele_chunks",
        "description": "Modèle XLM-RoBERTa fine-tuné pour l'analyse de sentiment multilingue"
    },
    "logistic": {
        "repo_name": f"{HF_USERNAME}/sentiment-model-logistic",
        "local_path": "./src/utils/bestmodel.pkl",
        "description": "Modèle de régression logistique pour l'analyse de sentiment"
    }
}

def upload_finetuned_model():
    """Upload du modèle fine-tuned (fichiers .json, .safetensors, .bpe.model)"""
    print("Upload du modèle fine-tuned...")
    
    config = MODELS_CONFIG["finetuned"]
    
    # Créer le repo
    api = HfApi(token=HF_TOKEN)
    try:
        create_repo(
            repo_id=config["repo_name"],
            token=HF_TOKEN,
            repo_type="model",
            exist_ok=True
        )
        print(f"Repo créé: {config['repo_name']}")
    except Exception as e:
        print(f"⚠️  Repo existe déjà ou erreur: {e}")
    
    # Créer un README.md
    readme_content = f"""---
license: mit
tags:
- sentiment-analysis
- french
- multilingual
- xlm-roberta
language:
- fr
- en
datasets:
- custom
metrics:
- accuracy
---

# {config['repo_name']}

{config['description']}

## Usage

```python
from transformers import pipeline

classifier = pipeline(
    "sentiment-analysis", 
    model="{config['repo_name']}", 
    device=-1, 
    truncation=True, 
    max_length=514
)

result = classifier("Ce commentaire est fantastique!")
print(result)
```

## Model Details

- **Base Model**: cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual
- **Fine-tuned on**: Custom French YouTube comments dataset
- **Task**: Sentiment Analysis (3 classes: POSITIVE, NEGATIVE, NEUTRAL)
"""
    
    # Sauvegarder le README
    readme_path = Path(config["local_path"]) / "README.md"
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(readme_content)
    
    # Upload du dossier complet
    upload_folder(
        folder_path=config["local_path"],
        repo_id=config["repo_name"],
        token=HF_TOKEN,
        commit_message="Initial upload of fine-tuned sentiment model"
    )
    print(f"Modèle fine-tuned uploadé vers {config['repo_name']}")

def upload_logistic_model():
    """Upload du modèle logistic regression avec script de chargement"""
    print("Upload du modèle logistic regression...")
    
    config = MODELS_CONFIG["logistic"]
    
    # Créer le repo
    api = HfApi(token=HF_TOKEN)
    try:
        create_repo(
            repo_id=config["repo_name"],
            token=HF_TOKEN,
            repo_type="model",
            exist_ok=True
        )
        print(f"Repo créé: {config['repo_name']}")
    except Exception as e:
        print(f"Repo existe déjà ou erreur: {e}")
    
    # Créer un dossier temporaire pour le modèle
    temp_dir = Path("./temp_logistic")
    temp_dir.mkdir(exist_ok=True)
    
    # Copier le modèle pickle
    import shutil
    shutil.copy2(config["local_path"], temp_dir / "model.pkl")
    
    # Créer un script de chargement
    loading_script = '''"""
Script de chargement pour le modèle logistic regression
"""
import pickle
import numpy as np
from huggingface_hub import hf_hub_download

def load_logistic_model(repo_id="Carlito-25/sentiment-model-logistic"):
    """Charge le modèle logistic regression depuis Hugging Face"""
    model_path = hf_hub_download(
        repo_id=repo_id,
        filename="model.pkl"
    )
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    return model

def predict_sentiment(model, text_features):
    """Prédiction avec le modèle logistic regression"""
    if isinstance(text_features, list):
        text_features = np.array(text_features).reshape(1, -1)
    
    prediction = model.predict(text_features)
    probabilities = model.predict_proba(text_features)
    
    return {
        'prediction': prediction[0],
        'probabilities': probabilities[0].tolist()
    }

# Exemple d'usage
if __name__ == "__main__":
    model = load_logistic_model()
    # Remplace par tes features réelles
    dummy_features = np.random.rand(1, 100)  # Adapte selon tes features
    result = predict_sentiment(model, dummy_features)
    print(result)
'''
    
    with open(temp_dir / "load_model.py", "w", encoding="utf-8") as f:
        f.write(loading_script)
    
    # Créer un README.md
    readme_content = f"""---
license: mit
tags:
- sentiment-analysis
- logistic-regression
- sklearn
- french
language:
- fr
---

# {config['repo_name']}

{config['description']}

## Usage

```python
from load_model import load_logistic_model, predict_sentiment
import numpy as np

# Charger le modèle
model = load_logistic_model()

# Prédiction (remplace par tes vraies features)
features = np.array([...])  # Tes features TF-IDF ou Word2Vec
result = predict_sentiment(model, features)
print(result)
```

## Model Details

- **Algorithm**: Logistic Regression (scikit-learn)
- **Features**: TF-IDF/Word2Vec vectors
- **Task**: Sentiment Analysis
- **Training Data**: Custom French YouTube comments dataset
"""
    
    with open(temp_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)
    
    # Upload du dossier
    upload_folder(
        folder_path=str(temp_dir),
        repo_id=config["repo_name"],
        token=HF_TOKEN,
        commit_message="Initial upload of logistic regression sentiment model"
    )
    
    # Nettoyer le dossier temporaire
    shutil.rmtree(temp_dir)
    print(f"Modèle logistic regression uploadé vers {config['repo_name']}")

def main():
    """Upload tous les modèles"""
    if not HF_TOKEN:
        print("Erreur: Variable d'environnement HF_TOKEN non définie")
        print("Créez un token sur https://huggingface.co/settings/tokens")
        print("Puis: export HF_TOKEN=your_token_here")
        return
    
    print("Configuration des modèles:")
    for model_name, config in MODELS_CONFIG.items():
        print(f"  - {model_name}: {config['repo_name']}")
    
    # Vérifier que les fichiers existent
    if not Path(MODELS_CONFIG["finetuned"]["local_path"]).exists():
        print(f"Dossier {MODELS_CONFIG['finetuned']['local_path']} introuvable")
        return
    
    if not Path(MODELS_CONFIG["logistic"]["local_path"]).exists():
        print(f"Fichier {MODELS_CONFIG['logistic']['local_path']} introuvable")
        print("Assure-toi d'avoir sauvegardé ton modèle logistic regression en pickle")
        return
    
    try:
        upload_finetuned_model()
        upload_logistic_model()
        print("\n🎉 Tous les modèles ont été uploadés avec succès!")
        print("\nProchaines étapes:")
        print("1. Vérifie tes repos sur https://huggingface.co/Carlito-25")
        print("2. Test le chargement des modèles")
        print("3. Intègre le nouveau système de modèles dans ton code")
        
    except Exception as e:
        print(f"Erreur lors de l'upload: {e}")

if __name__ == "__main__":
    main()