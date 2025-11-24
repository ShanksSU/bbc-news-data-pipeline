from nltk.sentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
import pandas as pd

def run_sentiment_analysis_vader(
    mongo_output_collection_name="articles_sentiment_vader"
):
    # run vader sentiment analysis (vader_lexicon preinstalled in dockerfile)
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    df = pd.DataFrame(list(db["articles_processed"].find({})))
    if df.empty:
        print("[vader] no processed articles found.")
        return

    sia = SentimentIntensityAnalyzer()

    df["sentiment_score"] = df["article_clean"].apply(
        lambda txt: sia.polarity_scores(txt)["compound"]
    )

    def label(score: float) -> str:
        # map score to label
        if score >= 0.05:
            return "positive"
        if score <= -0.05:
            return "negative"
        return "neutral"

    df["sentiment_label"] = df["sentiment_score"].apply(label)
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    db[mongo_output_collection_name].delete_many({})
    db[mongo_output_collection_name].insert_many(df.to_dict("records"))

    print("[vader] sentiment analysis completed.")