# BBC News Scraper & NLP Pipeline

A fully automated BBC News analytics pipeline built with Apache Airflow as the orchestration backbone, integrating Scrapy crawling, MongoDB storage, NLP modules (text preprocessing, topic modeling, sentiment & emotion analysis), statistical visualization, and containerized deployment through Docker Compose.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Tech Stack](#tech-stack)
4. [Directory Structure](#directory-structure)
5. [Environment Requirements](#environment-requirements)
6. [How to Start the Project](#how-to-start-the-project)
7. [Airflow DAG Workflow](#airflow-dag-workflow)
8. [Scrapy Crawler Overview](#scrapy-crawler-overview)
9. [NLP / Sentiment Analysis / Topic Modeling](#nlp--sentiment-analysis--topic-modeling)
10. [Visualization & Reports](#visualization--reports)
11. [MongoDB Collections](#mongodb-collections)

---

## Project Overview

This project builds a complete, automated data pipeline for BBC News that periodically performs:

1. Automatically collecting latest BBC news URLs via sitemap

2. Crawling BBC News articles using Scrapy and storing them in MongoDB

3. Performing NLP preprocessing (cleaning, POS filtering, lemmatization)

4. Running multiple LDA topic models (12 / 32 topics)

5. Running three sentiment & emotion analysis models:

   * **VADER** (rule-based sentiment analysis)
   * **DistilBERT** (binary sentiment classification)
   * **DistilRoBERTa** (multi-emotion classification)

6. Automatically generating visual analytics (Bar, Pie, Trend, Heatmap, WordCloud, etc.)

7. Using Apache Airflow as the orchestrator for workflow control, error handling, retries, and scheduling

The entire system is deployed with a single command via Docker Compose and features a modular design that makes it easy to extend new news sources, NLP models, or statistical modules.

---

## System Architecture

The following diagram illustrates the data flow and interaction between modules:

```
Image placeholder
```

The Airflow container communicates through the Docker network with:

* `mongo` (news database)
* `postgres` (Airflow metadata)
* `redis` (Celery broker)

---

## Tech Stack

### Workflow & Scheduling

* Apache Airflow 2.10.x
* Celery Executor
* Redis (Broker)
* PostgreSQL (Airflow Metadata)

### Crawling

* Scrapy
* BeautifulSoup / lxml (sitemap parsing)

### Storage Layer

* MongoDB
* Docker volume persistence

### NLP Models

* NLTK (stopwords, tokenization, VADER)
* Gensim (LDA, coherence calculation)
* HuggingFace Transformers (DistilBERT / DistilRoBERTa)
* Pandas / NumPy

### Visualization

* Matplotlib
* Seaborn
* WordCloud
* pyLDAvis (HTML topic visualization)

### Containerization

* Docker
* Docker Compose
* Custom Airflow Dockerfile (with NLTK data, Scrapy, Transformers)

---

## Directory Structure

```
.
├─ .dockerignore
├─ .env
├─ docker-compose.yaml
├─ Dockerfile
├─ README.md
├─ requirements.txt
│
├─ airflow/
│  ├─ dags/
│  │  ├─ bbc_news_dag.py                  # Main Airflow DAG (scraping + NLP + visualization)
│  │  ├─ test_connection_dag.py           # Test DAG
│  │  └─ output/                          # Visualization outputs
│  │       └─ YYYY-MM-DD_HHMM/
│  │           ├─ vader/
│  │           ├─ bert/
│  │           ├─ distilroberta/
│  │           └─ topics/
│  │
│  ├─ logs/                               # Airflow logs
│  │   ├─ dag_id=bbc_news_pipeline/
│  │   ├─ dag_id=test_connection_dag/
│  │   ├─ dag_processor_manager/
│  │   └─ scheduler/
│  │
│  ├─ models/                              # LDA model outputs
│  │   ├─ 12_YYYY-MM-DD_HHMM/
│  │   └─ 32_YYYY-MM-DD_HHMM/
│  │
│  ├─ plugins/
│  │  └─ nlp_tasks/                        # NLP modules (Airflow plugin)
│  │       ├─ archive_scraper.py
│  │       ├─ data_preparation.py
│  │       ├─ sentiment_analysis.py
│  │       ├─ sentiment_analysis_bert.py
│  │       ├─ sentiment_analysis_DistilRoBERTa.py
│  │       ├─ sentiment_analysis_vader.py
│  │       ├─ stats.py
│  │       ├─ stats_visualization.py
│  │       └─ topic_modeling.py
│  │
│  └─ scraper/
│      ├─ scrapy.cfg
│      └─ bbcNews/
│          ├─ items.py
│          ├─ middlewares.py
│          ├─ pipelines.py
│          ├─ settings.py
│          └─ spiders/
│               └─ ArticlesSpider.py
│
├─ data/
│  └─ mongo/                                # MongoDB volume data
│
└─ temp/                                     # User temp data
```

---

## Environment Requirements

* Docker
* Docker Compose
* 8GB+ RAM (12GB+ recommended for Transformer inference)

---

## How to Start the Project

### Step 1: Create `.env`

In the project root:

```
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### Step 2: Start all containers

```
docker compose up -d --build
```

This launches:

* Airflow (webserver / scheduler / worker / triggerer)
* MongoDB
* Redis
* PostgreSQL
* Airflow initial setup (auto-create admin user)

### Step 3: Access Airflow UI

* URL: [http://localhost:8080](http://localhost:8080)
* User: **airflow**
* Password: **airflow**

Enable the `bbc_news_pipeline` DAG.

---

## Airflow DAG Workflow

Main DAG: **bbc_news_dag.py**

### Task List

| Task ID                 | Function                                             |
| ----------------------- | ---------------------------------------------------- |
| get_docs_count          | Count existing links and articles in Mongo           |
| collect_urls            | Parse BBC sitemap and insert URLs into `mongo.links` |
| crawl_news              | Run Scrapy to fetch latest articles                  |
| data_preparation        | Text cleaning, POS filtering, lemmatization          |
| topic_model_32          | Train 32-topic LDA model                             |
| topic_model_12          | Train 12-topic LDA model                             |
| extract_lda_words       | Extract top words for topics                         |
| sentiment_vader         | VADER sentiment scoring                              |
| sentiment_bert          | DistilBERT sentiment classification                  |
| sentiment_distilroberta | DistilRoBERTa emotion classification                 |
| push_statistics         | Push NLP stats to XCom                               |
| visualize_pipeline      | Generate full visualization report                   |

### DAG Flow Diagram

```
DAG diagram placeholder
```

---

## Scrapy Crawler Overview

Scrapy project path: `airflow/scraper/bbcNews/`

Crawler workflow:

1. Read URLs from `Mongo.links`
2. Parse different BBC HTML layouts
3. Extract metadata: title, subtitle, author, paragraphs, tags, images
4. Build article item
5. Pipeline operations:

   * Field validation
   * Insert into `Mongo.NewsSpider`
   * Duplicate prevention (unique index + Skip Middleware)

Crawler behavior can be configured in `settings.py`.

---

## NLP / Sentiment Analysis / Topic Modeling

All NLP modules are under:
`airflow/plugins/nlp_tasks/`

---

### 1. Text Preprocessing (data_preparation.py)

Includes:

* Lowercasing
* Removing URLs and HTML tags
* Removing noise strings and stopwords
* POS filtering (nouns/adjectives/adverbs only)
* WordNet lemmatization
* Removing short documents (< 50 words)

Output: `articles_processed`

---

### 2. LDA Topic Modeling (topic_modeling.py)

* Train Gensim LDA (12 / 32 topics)
* Coherence scoring
* pyLDAvis HTML export
* Assign dominant topic per article

Output: `articles_with_topics`

---

### 3. Sentiment & Emotion Analysis

| Model         | Description                                                    | Mongo Collection                 |
| ------------- | -------------------------------------------------------------- | -------------------------------- |
| VADER         | Rule-based sentiment scores (neg/mid/pos)                      | `articles_sentiment_vader`       |
| DistilBERT    | HuggingFace binary sentiment classifier                        | `articles_sentiment_bert`        |
| DistilRoBERTa | Multi-class emotion classification (anger, joy, sadness, etc.) | `articles_emotion_distilroberta` |

---

## Visualization & Reports

Generated by: `stats_visualization.py`
Output directory:

```
/opt/airflow/dags/output/{YYYY-MM-DD_HHMM}/
                           ├─ vader/
                           ├─ bert/
                           └─ distilroberta/
                           └─ topics/
```

Includes:

* Topic distribution bar charts
* Topic heatmaps (date × topic)
* Topic word clouds
* Sentiment/emotion distribution (Pie/Bar)
* Multi-day emotion trends
* KDE, histograms, boxplots
* Scatter (text length vs sentiment)