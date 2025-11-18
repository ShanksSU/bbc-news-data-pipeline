# sentiment_analysis_DistilRoBERTa.py

from transformers import pipeline
from pymongo import MongoClient
import pandas as pd
import torch


def run_sentiment_analysis_distilroberta(mongo_output_collection_name: str = "articles_emotion_distilroberta",):
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    articles = list(db["articles_processed"].find({}))
    df = pd.DataFrame(articles)

    if df.empty:
        print("No processed articles found. Skipping emotion classification.")
        return

    device = 0 if torch.cuda.is_available() else -1

    emotion_classifier = pipeline(
        "text-classification",
        model="j-hartmann/emotion-english-distilroberta-base",
        return_all_scores=False,
        device=device,
    )

    results = emotion_classifier(df["article_clean"].tolist(), truncation=True)

    df["emotion_label"] = [r["label"].lower() for r in results]
    df["emotion_score"] = [float(r["score"]) for r in results]

    df.drop(columns=["_id"], inplace=True, errors="ignore")

    db[mongo_output_collection_name].delete_many({})
    db[mongo_output_collection_name].insert_many(df.to_dict("records"))

    print(
        f"DistilRoBERTa Emotion Analysis finished → saved to '{mongo_output_collection_name}'"
    )