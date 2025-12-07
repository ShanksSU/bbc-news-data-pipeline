import os
import glob
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Iterable

import pandas as pd
from pymongo import MongoClient

# Create a unique folder name if the target already exists
# Example: topic_12 -> topic_12_1 -> topic_12_2 ...
def create_unique_dir(base_dir: str) -> str:
    if not os.path.exists(base_dir):
        return base_dir
    n = 1
    while os.path.exists(f"{base_dir}_{n}"):
        n += 1
    return f"{base_dir}_{n}"

# Build topic outputs:
# - topics: {topic_id: [word1, word2, ...]}
# - topic_words_df: one row per (topic_id, rank, word, weight)
# - topics_summary: readable text for logs/XCom
def _build_topics_outputs(lda_model, num_topics: int, topn: int = 10) -> Dict[str, Any]:
    topics: Dict[int, List[str]] = {}
    rows = []

    for topic_id in range(num_topics):
        # Top words of one topic: [(word, weight), ...]
        top_words = lda_model.show_topic(topic_id, topn=topn)
        topics[topic_id] = [w for w, _ in top_words]

        # Save each word as a row (with rank and weight)
        for rank, (word, weight) in enumerate(top_words, start=1):
            rows.append(
                {"topic_id": topic_id, "rank": rank, "word": word, "weight": float(weight)}
            )

    topic_words_df = pd.DataFrame(rows)

    # Make a readable summary string
    lines = [f"Topic {num_topics}："]
    for topic_id in range(num_topics):
        lines.append(f"Topic #{topic_id}:")
        lines.append(" ".join(topics[topic_id]))
    topics_summary = "\n".join(lines)

    return {
        "topics": topics,
        "topic_words_df": topic_words_df,
        "topics_summary": topics_summary,
    }

# Find the latest run directory under /opt/airflow/models
# Pattern example: /opt/airflow/models/YYYY-mm-dd_HHMM
def _find_latest_run_dir(models_root: str = "/opt/airflow/models") -> Optional[str]:
    candidates = [p for p in glob.glob(os.path.join(models_root, "*_*")) if os.path.isdir(p)]
    if not candidates:
        return None
    # Pick the newest folder by modified time
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

# Find the latest topic folder for a given k inside a run_dir
# Example: topic_12, topic_12_1 ...
def _find_latest_topic_dir(num_topics: int, run_dir: Optional[str] = None, models_root: str = "/opt/airflow/models") -> Optional[str]:
    # If run_dir not given, use the latest run_dir
    if run_dir is None:
        run_dir = _find_latest_run_dir(models_root=models_root)
    if not run_dir:
        return None

    pattern = os.path.join(run_dir, f"topic_{num_topics}*")
    candidates = [p for p in glob.glob(pattern) if os.path.isdir(p)]
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def run_topic_modeling(
    output_collection: str = "articles_with_topics",
    num_topics: int = 12,
    save_vis: bool = True,
    topn_words: int = 10,

    # Shared run_dir name (same DAG run should pass the same value)
    run_dir_name: Optional[str] = None,          # e.g. "2025-12-08_1430"
    models_root: str = "/opt/airflow/models",

    # Auto-tune settings (scan k and select the best coherence)
    auto_tune: bool = False,
    topic_candidates: Optional[Iterable[int]] = None,
    topic_min: int = 8,
    topic_max: int = 40,
    topic_step: int = 2,

    # Faster settings for scanning
    scan_passes: int = 5,
    scan_iterations: int = 200,

    # More stable settings for final training
    final_passes: int = 10,
    final_iterations: int = 400,

    # Priors for better interpretability
    alpha: str = "asymmetric",
    eta: str = "auto",
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

    # Simple tokenization + stopword removal
    stop_words = set(stopwords.words("english"))
    texts: List[List[str]] = [
        [w for w in doc.lower().split() if w not in stop_words]
        for doc in df["article_clean"]
    ]
    
    # Build dictionary and corpus (bag-of-words)
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=5, no_above=0.5, keep_n=50000)
    dictionary.compactify()
    corpus = [dictionary.doc2bow(t) for t in texts]

    # Build run root folder: /opt/airflow/models/YYYY-mm-dd_HHMM
    if not run_dir_name:
        run_dir_name = datetime.now().strftime("%Y-%m-%d_%H%M")

    run_root = os.path.join(models_root, run_dir_name)
    os.makedirs(run_root, exist_ok=True)

    scan_results: List[Dict[str, Any]] = []
    best_k = num_topics
    best_coh = -1.0

    # Auto-tune: scan different k and pick the best coherence
    if auto_tune:
        if topic_candidates is None:
            topic_candidates = list(range(topic_min, topic_max + 1, topic_step))
        else:
            topic_candidates = list(topic_candidates)

        print(f"[TopicModel] Auto-tune enabled. Candidates: {topic_candidates}")

        for k in topic_candidates:
            print(f"[TopicModel] Scanning k={k} ...")
            scan_model = models.LdaModel(
                corpus=corpus,
                num_topics=k,
                id2word=dictionary,
                passes=scan_passes,
                iterations=scan_iterations,
                alpha=alpha,
                eta=eta,
                chunksize=2000,
                random_state=42,
                eval_every=None,
            )

            coh = CoherenceModel(
                model=scan_model,
                texts=texts,
                dictionary=dictionary,
                coherence="c_v",
                processes=1
            ).get_coherence()

            scan_results.append({"num_topics": k, "coherence": float(coh)})

            if coh > best_coh:
                best_coh = coh
                best_k = k

        print(f"[TopicModel] Best k = {best_k}, coherence = {best_coh:.4f}")
        num_topics = best_k  # Use best_k for the final model

    # Train final LDA model
    print(f"[TopicModel] Training FINAL LDA: {num_topics} topics...")
    lda_model = models.LdaModel(
        corpus=corpus,
        num_topics=num_topics,
        id2word=dictionary,
        passes=final_passes,
        iterations=final_iterations,
        alpha=alpha,
        eta=eta,
        chunksize=2000,
        random_state=42,
        eval_every=None,
    )
    
    # Compute coherence for the final model
    coherence = CoherenceModel(
        model=lda_model,
        texts=texts,
        dictionary=dictionary,
        coherence="c_v",
        processes=1
    ).get_coherence()

    # Output folder: /opt/airflow/models/run_dir/topic_{k}/
    base_topic_dir = os.path.join(run_root, f"topic_{num_topics}")
    output_dir = create_unique_dir(base_topic_dir)   # Avoid overwrite if same k exists
    os.makedirs(output_dir, exist_ok=True)

    # Save model files
    model_path = os.path.join(output_dir, f"lda_{num_topics}.model")
    dict_path = os.path.join(output_dir, f"lda_{num_topics}.dict")
    html_path = os.path.join(output_dir, f"lda_vis_{num_topics}.html")

    lda_model.save(model_path)
    dictionary.save(dict_path)

    if save_vis:
        vis = gensimvis.prepare(lda_model, corpus, dictionary)
        pyLDAvis.save_html(vis, html_path)
        print(f"[TopicModel] LDA vis saved → {html_path}")

    # Save auto-tune scan results as CSV (only when auto_tune=True)
    scan_csv_path = ""
    if auto_tune and scan_results:
        scan_df = pd.DataFrame(scan_results).sort_values("num_topics")
        scan_csv_path = os.path.join(output_dir, "coherence_scan.csv")
        scan_df.to_csv(scan_csv_path, index=False, encoding="utf-8-sig")

    # Assign the main topic for each document
    df["main_topic"] = [
        max(lda_model.get_document_topics(bow), key=lambda x: x[1])[0]
        for bow in corpus
    ]

    # Build topic words outputs and attach topic words to each article
    topic_pack = _build_topics_outputs(lda_model, num_topics=num_topics, topn=topn_words)
    topics = topic_pack["topics"]

    # Map topic_id -> "word1 word2 ..."
    topic_words_str_map = {tid: " ".join(words) for tid, words in topics.items()}
    df["main_topic_words"] = df["main_topic"].map(topic_words_str_map).fillna("")

    # Save list as JSON string (easy to load back)
    df["main_topic_words_list"] = df["main_topic"].map(topics).apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else "[]"
    )

    topic_words_df: pd.DataFrame = topic_pack["topic_words_df"]
    topics_summary: str = topic_pack["topics_summary"]

    # Save topic words table
    topics_csv_path = os.path.join(output_dir, f"topic_words_top{topn_words}.csv")
    topic_words_df.to_csv(topics_csv_path, index=False, encoding="utf-8-sig")

    # Save each topic words as separate CSV
    per_topic_dir = os.path.join(output_dir, "topics")
    os.makedirs(per_topic_dir, exist_ok=True)
    for topic_id in range(num_topics):
        sub = topic_words_df[topic_words_df["topic_id"] == topic_id][["rank", "word", "weight"]]
        sub.to_csv(os.path.join(per_topic_dir, f"topic_{topic_id}.csv"), index=False, encoding="utf-8-sig")

    # Save articles with main_topic and topic words
    articles_csv_path = os.path.join(output_dir, f"articles_with_main_topic_{num_topics}.csv")
    df.to_csv(articles_csv_path, index=False, encoding="utf-8-sig")

    # Save results back to MongoDB
    df.drop(columns=["_id"], inplace=True, errors="ignore")
    db[output_collection].delete_many({})
    db[output_collection].insert_many(df.to_dict("records"))
    print(f"[TopicModel] Results saved to MongoDB → {output_collection}")

    # Return values for XCom usage
    return {
        "run_dir_name": run_dir_name,
        "run_root": run_root,
        "output_dir": output_dir,

        "num_topics": num_topics,
        "best_num_topics": best_k if auto_tune else num_topics,
        "coherence": round(float(coherence), 4),

        "model_path": model_path,
        "dict_path": dict_path,
        "html_path": html_path if save_vis else "",

        "topics_csv_path": topics_csv_path,
        "articles_csv_path": articles_csv_path,

        "scan_results": scan_results,
        "scan_csv_path": scan_csv_path,

        "topics": topics,
        "topics_summary": topics_summary,
    }


def get_lda_top_words(
    num_topics: int = 12,
    topn: int = 10,
    run_dir_name: Optional[str] = None,
    models_root: str = "/opt/airflow/models",
) -> Dict[int, List[str]]:

    from gensim import corpora, models
    
    # If run_dir_name is given, search inside that run folder
    run_dir = os.path.join(models_root, run_dir_name) if run_dir_name else None
    topic_dir = _find_latest_topic_dir(num_topics=num_topics, run_dir=run_dir, models_root=models_root)
    if not topic_dir:
        print(f"[TopicModel] No topic dir found for num_topics={num_topics}")
        return {}

    model_path = os.path.join(topic_dir, f"lda_{num_topics}.model")
    dict_path = os.path.join(topic_dir, f"lda_{num_topics}.dict")

    if not os.path.exists(model_path) or not os.path.exists(dict_path):
        print(f"[TopicModel] Model/dict not found under: {topic_dir}")
        return {}

    lda_model = models.LdaModel.load(model_path)
    _ = corpora.Dictionary.load(dict_path)

    return {
        topic_id: [word for word, _ in lda_model.show_topic(topic_id, topn=topn)]
        for topic_id in range(num_topics)
    }
