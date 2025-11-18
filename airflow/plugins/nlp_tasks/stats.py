from pymongo import MongoClient
import pandas as pd

def push_stats_to_xcom(ti=None):
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    stats = {}

    # Topic counts
    for col in ["articles_with_topics"]:
        if col in db.list_collection_names():
            df = pd.DataFrame(list(db[col].find({})))
            if not df.empty and "main_topic" in df.columns:
                topic_counts = df["main_topic"].value_counts().to_dict()
                stats["topic_counts"] = topic_counts

    # Sentiment counts
    for col in ["articles_with_sentiment", "articles_with_sentiment_v2"]:
        if col in db.list_collection_names():
            df = pd.DataFrame(list(db[col].find({})))
            if not df.empty and "sentiment_label" in df.columns:
                sentiment_counts = df["sentiment_label"].value_counts().to_dict()
                stats[f"{col}_counts"] = sentiment_counts

    # Push stats to XCom
    if ti:
        ti.xcom_push(key="pipeline_stats", value=stats)

    print("Pipeline statistics pushed to XCom:")
    print(stats)
