import os
from datetime import datetime
from typing import Dict, Any, List

from pymongo import MongoClient
import pandas as pd

def create_unique_dir(base_dir: str) -> str:
    if not os.path.exists(base_dir):
        return base_dir
    n = 1
    while os.path.exists(f"{base_dir}_{n}"):
        n += 1
    return f"{base_dir}_{n}"


def run_topic_modeling(
    output_collection: str = "articles_with_topics",
    num_topics: int = 12,
    save_vis: bool = True
) -> Dict[str, Any]:

    # heavy imports lazy-load
    from gensim import corpora, models
    from gensim.models import CoherenceModel
    from nltk.corpus import stopwords
    import pyLDAvis
    import pyLDAvis.gensim_models as gensimvis

    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    df = pd.DataFrame(list(db["articles_processed"].find({})))
    if df.empty:
        print("[TopicModel] No processed articles found.")
        return {"error": "NO_DATA"}

    stop_words = set(stopwords.words("english"))

    texts: List[List[str]] = [
        [w for w in doc.lower().split() if w not in stop_words]
        for doc in df["article_clean"]
    ]

    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(t) for t in texts]

    print(f"[TopicModel] Training LDA: {num_topics} topics...")
    lda_model = models.LdaModel(
        corpus=corpus,
        num_topics=num_topics,
        id2word=dictionary,
        passes=10,
        chunksize=2000,
        random_state=42,
    )

    coherence = CoherenceModel(
        model=lda_model,
        texts=texts,
        dictionary=dictionary,
        coherence="c_v",
        processes=1
    ).get_coherence()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    base_dir = f"/opt/airflow/models/{num_topics}_{timestamp}"
    output_dir = create_unique_dir(base_dir)
    os.makedirs(output_dir, exist_ok=True)

    model_path = os.path.join(output_dir, f"lda_{num_topics}.model")
    dict_path = os.path.join(output_dir, f"lda_{num_topics}.dict")
    html_path = os.path.join(output_dir, f"lda_vis_{num_topics}.html")

    lda_model.save(model_path)
    dictionary.save(dict_path)

    if save_vis:
        vis = gensimvis.prepare(lda_model, corpus, dictionary)
        pyLDAvis.save_html(vis, html_path)
        print(f"[TopicModel] LDA vis saved → {html_path}")

    df["main_topic"] = [
        max(lda_model.get_document_topics(bow), key=lambda x: x[1])[0]
        for bow in corpus
    ]

    df.drop(columns=["_id"], inplace=True, errors="ignore")
    db[output_collection].delete_many({})
    db[output_collection].insert_many(df.to_dict("records"))

    print(f"[TopicModel] Results saved to MongoDB → {output_collection}")

    return {
        "num_topics": num_topics,
        "coherence": round(coherence, 4),
        "model_path": model_path,
        "dict_path": dict_path,
        "html_path": html_path
    }


def get_lda_top_words(num_topics: int = 12, topn: int = 10) -> Dict[int, List[str]]:
    from gensim import corpora, models

    model_path = f"/opt/airflow/models/lda_{num_topics}.model"
    dict_path = f"/opt/airflow/models/lda_{num_topics}.dict"

    if not os.path.exists(model_path):
        print(f"[TopicModel] Model not found: {model_path}")
        return {}

    lda_model = models.LdaModel.load(model_path)
    dictionary = corpora.Dictionary.load(dict_path)

    return {
        topic_id: [word for word, _ in lda_model.show_topic(topic_id, topn=topn)]
        for topic_id in range(num_topics)
    }
