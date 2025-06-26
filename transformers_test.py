try:
    from transformers import pipeline
    print("Import réussi ✅")
except Exception as e:
    print("Erreur lors de l'import :", e)