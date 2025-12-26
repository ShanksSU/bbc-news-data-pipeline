from datetime import datetime
import os
import subprocess
import requests
from bs4 import BeautifulSoup as bs
import pymongo

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from nlp_tasks.data_preparation import process
from nlp_tasks.topic_modeling import run_topic_modeling as topic_model
from nlp_tasks.topic_modeling import get_lda_top_words
from nlp_tasks.sentiment_analysis import (
    run_vader_wrapper, 
    run_bert_wrapper, 
    run_distilroberta_wrapper
)
from nlp_tasks.stats import push_stats_to_xcom
from nlp_tasks.stats_visualization import visualize_pipeline_stats

BASE_URL = "https://www.bbc.com/sitemaps/https-index-com-news.xml"

# Remove duplicated URLs in MongoDB collection
def remove_duplicate_urls(collection):
    pipeline = [
        {"$group": {
            "_id": "$url",
            "ids": {"$addToSet": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = list(collection.aggregate(pipeline))
    removed = 0

    for doc in duplicates:
        ids = doc["ids"]
        for _id in ids[1:]:
            collection.delete_one({"_id": _id})
            removed += 1

    if removed > 0:
        print(f"[DEDUPE] Removed {removed} duplicated URLs.")
    else:
        print("[DEDUPE] No duplicates found.")

# Parse one child sitemap and insert news URLs into MongoDB
def parse_sitemap(url, collection):
    print(f"[SITEMAP] Parsing: {url}")

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"[ERROR] Cannot fetch sitemap: {e}")
        return

    if response.status_code != 200:
        print(f"[WARN] Skipping sitemap {url}, HTTP {response.status_code}")
        return

    soup = bs(response.text, "lxml")
    loc_elements = soup.find_all("url")

    inserted = 0
    for loc in loc_elements:
        page = loc.loc.text
        if "www.bbc.com/news/" not in page:
            continue

        try:
            collection.insert_one({
                "lastmod": loc.lastmod.text if loc.lastmod else None,
                "url": page
            })
            inserted += 1
        except pymongo.errors.DuplicateKeyError:
            continue

    print(f"[SITEMAP] Inserted {inserted} URLs from {url}")

# Fetch root sitemap, then parse all child sitemaps
def get_urls_from_sitemap(root_sitemap):
    client = pymongo.MongoClient("mongo", 27017)
    db = client.bbcnews

    links_col = db["links"]

    remove_duplicate_urls(links_col)

    existing_indexes = links_col.index_information()
    if "url_1" not in existing_indexes:
        print("[INDEX] Creating unique index on links.url...")
        links_col.create_index("url", unique=True)
    else:
        print("[INDEX] Unique index already exists, skip.")

    print(f"[ROOT] Fetching root sitemap: {root_sitemap}")

    response = requests.get(root_sitemap)
    soup = bs(response.text, "lxml")

    child_sitemaps = [loc.text for loc in soup.find_all("loc")]
    print(f"[ROOT] Found {len(child_sitemaps)} sitemaps")

    for sm_url in child_sitemaps:
        parse_sitemap(sm_url, links_col)

    print("[ROOT] All links collected successfully.")

# Count documents in MongoDB collections
def get_docs_count():
    client = pymongo.MongoClient("mongo", 27017)
    db = client.bbcnews

    lc = db["links"].count_documents({})
    ac = db["NewsSpider"].count_documents({})

    print(f"[COUNT] links={lc}, articles={ac}")
    return {"links_count": lc, "articles_count": ac}

# Run Scrapy spider to crawl news pages
def crawl_news(**context):
    counts = context["ti"].xcom_pull(task_ids="get_docs_count") or {}
    docs_count = counts.get("links_count", 0)

    scrapy_path = "/opt/airflow/scraper"
    if not os.path.exists(os.path.join(scrapy_path, "scrapy.cfg")):
        raise FileNotFoundError("scrapy.cfg not found inside Airflow container.")

    print(f"[SCRAPY] Starting spider (docs_count={docs_count})")
    
    # call class NewsSpider(scrapy.Spider)
    result = subprocess.run(
        ["scrapy", "crawl", "NewsSpider", "-a", f"docs_count={docs_count}"],
        cwd=scrapy_path,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError("Scrapy crawl failed")

    print("[SCRAPY] Completed successfully.")


# Wrapper to avoid Jinja string issue and get counts from XCom
def data_preparation_wrapper(**context):
    counts = context["ti"].xcom_pull(task_ids="get_docs_count")
    return process(counts=counts)

# Train a fixed 12-topic model
def topic_model_12_wrapper(**context):
    return topic_model(
        num_topics=12,
        auto_tune=False,
        topn_words=10,
        save_vis=True,
        output_collection="articles_topic_12"
    )

# Train a fixed 32-topic model
def topic_model_32_wrapper(**context):
    return topic_model(
        num_topics=32,
        auto_tune=False,
        topn_words=10,
        save_vis=True,
        output_collection="articles_topic_32"
    )

# Auto-tune: scan k and pick the best number of topics
def topic_model_auto_wrapper(**context):
    return topic_model(
        auto_tune=True,
        topic_min=10,
        topic_max=40,
        topic_step=5,
        topn_words=10,
        save_vis=True,
        scan_passes=2,
        scan_iterations=50,
        final_passes=10,
        final_iterations=400,
        output_collection="articles_topic_auto"
    )

# Collect topic outputs and push to XCom
def publish_topic_outputs(**context):
    ti = context["ti"]

    # Get results from topic model tasks
    res12 = ti.xcom_pull(task_ids="topic_model_12") or {}
    res32 = ti.xcom_pull(task_ids="topic_model_32") or {}
    resa = ti.xcom_pull(task_ids="topic_model_auto") or {}

    # Push readable summaries to XCom
    ti.xcom_push(key="topics_summary_12", value=res12.get("topics_summary", ""))
    ti.xcom_push(key="topics_summary_32", value=res32.get("topics_summary", ""))
    ti.xcom_push(key="topics_summary_auto", value=resa.get("topics_summary", ""))

    # Push structured topics dict to XCom
    ti.xcom_push(key="topics_12", value=res12.get("topics", {}))
    ti.xcom_push(key="topics_32", value=res32.get("topics", {}))
    ti.xcom_push(key="topics_auto", value=resa.get("topics", {}))

    # Push article CSV paths to XCom
    ti.xcom_push(key="articles_csv_12", value=res12.get("articles_csv_path", ""))
    ti.xcom_push(key="articles_csv_32", value=res32.get("articles_csv_path", ""))
    ti.xcom_push(key="articles_csv_auto", value=resa.get("articles_csv_path", ""))

    # Push topic words CSV paths to XCom
    ti.xcom_push(key="topics_csv_12", value=res12.get("topics_csv_path", ""))
    ti.xcom_push(key="topics_csv_32", value=res32.get("topics_csv_path", ""))
    ti.xcom_push(key="topics_csv_auto", value=resa.get("topics_csv_path", ""))

    # Push auto-tune best k to XCom
    ti.xcom_push(key="best_k_auto", value=resa.get("best_num_topics", resa.get("num_topics", "")))
    ti.xcom_push(key="coherence_auto", value=resa.get("coherence", ""))

    # Optional: also push top words from latest saved models
    ti.xcom_push(key="lda_top_words_12", value=get_lda_top_words(num_topics=12, topn=10))
    ti.xcom_push(key="lda_top_words_32", value=get_lda_top_words(num_topics=32, topn=10))

    # Print summaries in task logs
    print("===== Topics 12 =====")
    print(res12.get("topics_summary", ""))
    print("===== Topics 32 =====")
    print(res32.get("topics_summary", ""))
    print("===== Topics AUTO =====")
    print(resa.get("topics_summary", ""))
    print(f"[AUTO] best_k={resa.get('best_num_topics', resa.get('num_topics'))}, coherence={resa.get('coherence')}")
    if resa.get("scan_csv_path"):
        print(f"[AUTO] coherence_scan.csv => {resa.get('scan_csv_path')}")

    return {"ok": True}


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    dag_id="bbc_news_pipeline",
    schedule_interval="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bbc", "nlp", "scraping"],
)

# Task: count docs in MongoDB
t1_count = PythonOperator(
    task_id="get_docs_count",
    python_callable=get_docs_count,
    dag=dag,
)

# Task: collect URLs from sitemap
t2_urls = PythonOperator(
    task_id="collect_urls",
    python_callable=get_urls_from_sitemap,
    op_kwargs={"root_sitemap": BASE_URL},
    dag=dag,
)

# Task: crawl news with Scrapy
t3_crawl = PythonOperator(
    task_id="crawl_news",
    python_callable=crawl_news,
    provide_context=True,
    dag=dag,
)

# Task: clean/process text data
t4_process = PythonOperator(
    task_id="data_preparation",
    python_callable=data_preparation_wrapper,
    provide_context=True,
    dag=dag,
)

# Task: topic model with 32 topics
t5_topic32 = PythonOperator(
    task_id="topic_model_32",
    python_callable=topic_model_32_wrapper,
    provide_context=True,
    dag=dag,
)

# Task: topic model with 12 topics
t6_topic12 = PythonOperator(
    task_id="topic_model_12",
    python_callable=topic_model_12_wrapper,
    provide_context=True,
    dag=dag,
)

# Task: auto-tune topic model (find best k)
t6b_topic_auto = PythonOperator(
    task_id="topic_model_auto",
    python_callable=topic_model_auto_wrapper,
    provide_context=True,
    dag=dag,
)

# Task: publish topic outputs to XCom
t7_publish = PythonOperator(
    task_id="publish_topic_outputs",
    python_callable=publish_topic_outputs,
    provide_context=True,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
)

# Task: VADER sentiment analysis
t8_vader = PythonOperator(
    task_id="sentiment_vader",
    python_callable=run_vader_wrapper,
    dag=dag,
)

# Task: BERT sentiment analysis
t9_bert = PythonOperator(
    task_id="sentiment_bert",
    python_callable=run_bert_wrapper,
    dag=dag,
)

# Task: DistilRoBERTa sentiment analysis
t10_emotions = PythonOperator(
    task_id="sentiment_distilroberta",
    python_callable=run_distilroberta_wrapper,
    dag=dag,
)

# Task: compute pipeline stats and push to XCom
t11_stats = PythonOperator(
    task_id="push_statistics",
    python_callable=push_stats_to_xcom,
    dag=dag,
)

# Task: visualize stats
t12_visualize = PythonOperator(
    task_id="visualize_pipeline",
    python_callable=visualize_pipeline_stats,
    dag=dag,
)

# Task dependencies
t1_count >> t2_urls >> t3_crawl >> t4_process
t4_process >> [t6_topic12, t5_topic32, t6b_topic_auto] >> t7_publish
t7_publish >> [t8_vader, t9_bert, t10_emotions] >> t11_stats >> t12_visualize