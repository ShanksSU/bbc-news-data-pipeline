from pymongo import MongoClient
import pandas as pd

# compute topic and sentiment stats and push to airflow xcom
def push_stats_to_xcom(ti=None):
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    stats = {}

    # topic counts
    for col in ["articles_with_topics"]:
        if col in db.list_collection_names():
            df = pd.DataFrame(list(db[col].find({})))
            if not df.empty and "main_topic" in df.columns:
                topic_counts = df["main_topic"].value_counts().to_dict()
                stats["topic_counts"] = topic_counts

    # sentiment counts
    for col in ["articles_with_sentiment", "articles_with_sentiment_v2"]:
        if col in db.list_collection_names():
            df = pd.DataFrame(list(db[col].find({})))
            if not df.empty and "sentiment_label" in df.columns:
                sentiment_counts = df["sentiment_label"].value_counts().to_dict()
                stats[f"{col}_counts"] = sentiment_counts

    # push stats to xcom
    if ti:
        ti.xcom_push(key="pipeline_stats", value=stats)

    print("pipeline statistics pushed to xcom:")
    print(stats)