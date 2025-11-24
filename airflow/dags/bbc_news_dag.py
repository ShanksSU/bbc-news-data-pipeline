from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator

from nlp_tasks.data_preparation import process
from nlp_tasks.topic_modeling import run_topic_modeling as topic_model
from nlp_tasks.topic_modeling import get_lda_top_words
from nlp_tasks.sentiment_analysis_vader import run_sentiment_analysis_vader
from nlp_tasks.sentiment_analysis_bert import run_sentiment_analysis_bert
from nlp_tasks.sentiment_analysis_DistilRoBERTa import run_sentiment_analysis_distilroberta
from nlp_tasks.stats import push_stats_to_xcom
from nlp_tasks.stats_visualization import visualize_pipeline_stats

BASE_URL = "https://www.bbc.com/sitemaps/https-index-com-news.xml"

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


# parse child sitemap files
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


# fetch all sitemap URLs
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


# Count documents
def get_docs_count():
    client = pymongo.MongoClient("mongo", 27017)
    db = client.bbcnews

    lc = db["links"].count_documents({})
    ac = db["NewsSpider"].count_documents({})

    print(f"[COUNT] links={lc}, articles={ac}")
    return {"links_count": lc, "articles_count": ac}


# crapy spider execution
def crawl_news(**context):
    import subprocess
    import os

    counts = context["ti"].xcom_pull(task_ids="get_docs_count")
    docs_count = counts["links_count"]

    scrapy_path = "/opt/airflow/scraper"

    if not os.path.exists(os.path.join(scrapy_path, "scrapy.cfg")):
        raise FileNotFoundError("scrapy.cfg not found inside Airflow container.")

    print(f"[SCRAPY] Starting spider (docs_count={docs_count})")

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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="bbc_news_pipeline",
    schedule_interval="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bbc", "nlp", "scraping"],
)

t1_count = PythonOperator(
    task_id="get_docs_count",
    python_callable=get_docs_count,
    dag=dag,
)

t2_urls = PythonOperator(
    task_id="collect_urls",
    python_callable=get_urls_from_sitemap,
    op_kwargs={"root_sitemap": BASE_URL},
    dag=dag,
)

t3_crawl = PythonOperator(
    task_id="crawl_news",
    python_callable=crawl_news,
    provide_context=True,
    dag=dag,
)

t4_process = PythonOperator(
    task_id="data_preparation",
    python_callable=process,
    op_kwargs={"counts": "{{ ti.xcom_pull('get_docs_count') }}"},
    dag=dag,
)

t5_topic32 = PythonOperator(
    task_id="topic_model_32",
    python_callable=lambda: topic_model(num_topics=32),
    dag=dag,
)

t6_topic12 = PythonOperator(
    task_id="topic_model_12",
    python_callable=lambda: topic_model(num_topics=12),
    dag=dag,
)

t7_lda_words = PythonOperator(
    task_id="extract_lda_words",
    python_callable=lambda ti: ti.xcom_push(
        key="lda_top_words",
        value=get_lda_top_words(num_topics=32),
    ),
    dag=dag,
)

t8_vader = PythonOperator(
    task_id="sentiment_vader",
    python_callable=run_sentiment_analysis_vader,
    dag=dag,
)

t9_bert = PythonOperator(
    task_id="sentiment_bert",
    python_callable=run_sentiment_analysis_bert,
    dag=dag,
)

t10_emotions = PythonOperator(
    task_id="sentiment_distilroberta",
    python_callable=run_sentiment_analysis_distilroberta,
    dag=dag,
)

t11_stats = PythonOperator(
    task_id="push_statistics",
    python_callable=push_stats_to_xcom,
    dag=dag,
)

t12_visualize = PythonOperator(
    task_id="visualize_pipeline",
    python_callable=visualize_pipeline_stats,
    dag=dag,
)

t1_count >> t2_urls >> t3_crawl >> t4_process
t4_process >> [t5_topic32, t6_topic12] >> t7_lda_words
t7_lda_words >> [t8_vader, t9_bert, t10_emotions] >> t11_stats >> t12_visualize