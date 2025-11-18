from transformers import pipeline
from pymongo import MongoClient
import pandas as pd
import torch


def run_sentiment_analysis_bert(mongo_output_collection_name: str = "articles_sentiment_bert",):
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    articles = list(db["articles_processed"].find({}))
    df = pd.DataFrame(articles)

    if df.empty:
        print("No processed articles found. Skipping BERT sentiment analysis.")
        return

    device = 0 if torch.cuda.is_available() else -1

    classifier = pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        device=device,
    )

    results = classifier(df["article_clean"].tolist(), truncation=True)

    df["sentiment_label"] = [r["label"].lower() for r in results]
    df["sentiment_score"] = [float(r["score"]) for r in results]

    df.drop(columns=["_id"], inplace=True, errors="ignore")

    db[mongo_output_collection_name].delete_many({})
    db[mongo_output_collection_name].insert_many(df.to_dict("records"))

    print(
        f"BERT Sentiment Analysis finished → saved to '{mongo_output_collection_name}'"
    )