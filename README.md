# BBC News Scraper & NLP Pipeline

A fully automated BBC News analytics pipeline powered by **Apache Airflow**, integrating Scrapy crawling, MongoDB storage, NLP preprocessing, topic modeling, sentiment & emotion analysis, and visualization.
The entire system is containerized and can be launched with **Docker Compose** in one command.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Tech Stack](#tech-stack)
4. [Directory Structure](#directory-structure)
5. [Requirements](#requirements)
6. [How to Start the Project](#how-to-start-the-project)
7. [Airflow DAG Workflow](#airflow-dag-workflow)
8. [Scrapy Crawler Overview](#scrapy-crawler-overview)
9. [NLP / Sentiment / Topic Modeling](#nlp--sentiment--topic-modeling)
10. [Visualization & Reports](#visualization--reports)

---

## Project Overview

This project builds a complete, automated pipeline for analyzing BBC News. The workflow is designed to run periodically and includes:

1. Fetching the latest BBC News article URLs from sitemap archives
2. Scraping article content using Scrapy and storing it in MongoDB
3. NLP preprocessing (cleaning, POS filtering, lemmatization)
4. Topic modeling with multiple LDA models (12 / 32 topics)
5. Running three types of sentiment & emotion analysis:

   * **VADER** — rule-based sentiment scoring
   * **DistilBERT** — binary sentiment classification
   * **DistilRoBERTa** — multi-emotion classification
6. Generating analytical charts (Bar, Pie, Trend, Heatmap, WordCloud)
7. Coordinating the full workflow using Airflow DAGs with retries and scheduling

The entire environment runs inside Docker, fully portable and easy to extend with additional data sources or models.

---

## System Architecture

Overall data flow and module interactions:

```
placeholder for system diagram
```

Airflow containers connect through Docker networking to:

* **mongo** (news database)
* **postgres** (Airflow metadata)
* **redis** (Celery broker)

---

## Tech Stack

### Workflow Orchestration

* Apache Airflow 2.10.x
* Celery Executor
* Redis (broker)
* PostgreSQL (metadata storage)

### Web Scraping

* Scrapy
* BeautifulSoup / lxml (for sitemap parsing)

### Storage Layer

* MongoDB
* Docker volumes for persistence

### NLP Models

* NLTK (stopwords, tokenization, VADER)
* Gensim (LDA modeling & coherence)
* Transformers (DistilBERT / DistilRoBERTa)
* Pandas / NumPy

### Visualization

* Matplotlib
* Seaborn
* WordCloud
* pyLDAvis

### Containerization

* Docker
* Docker Compose
* Custom Airflow Dockerfile (preinstalled NLTK data, Scrapy, Transformers)

---

## Directory Structure

```
(unchanged structure)
```

---

## Requirements

* Docker
* Docker Compose
* At least **8GB RAM** (12GB+ recommended for Transformer inference)

---

## How to Start the Project

### **Step 0 (Optional): Clean existing containers & volumes**

```bash
docker compose down --volumes --remove-orphans
```

---

### **Step 1: Create the `.env` file**

In the project root:

```
AIRFLOW_UID=0
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

---

### **Step 2: Build and start the containers**

To fully rebuild images:

```bash
docker-compose build --no-cache
docker-compose up --build -d
```

Or the standard startup:

```bash
docker compose up -d --build
```

This launches:

* Airflow (webserver / scheduler / worker / triggerer)
* MongoDB
* Redis
* PostgreSQL
* Airflow initialization (auto-create admin user)

---

### **Step 3: Restart services (if DAG or Dockerfile changed)**

```bash
docker compose restart
```

---

### **Step 4: Open Airflow UI**

* URL: [http://localhost:8080](http://localhost:8080)
* Username: `airflow`
* Password: `airflow`

Enable the **bbc_news_pipeline** DAG.

---

## Airflow DAG Workflow

Main DAG: `bbc_news_dag.py`

### Task List

| Task ID                 | Description                                         |
| ----------------------- | --------------------------------------------------- |
| get_docs_count          | Count current `links` & scraped articles in MongoDB |
| collect_urls            | Parse BBC sitemap and store URLs into Mongo.links   |
| crawl_news              | Run Scrapy to fetch latest articles                 |
| data_preparation        | Text cleaning, POS filtering, lemmatization         |
| topic_model_32          | Train LDA with 32 topics                            |
| topic_model_12          | Train LDA with 12 topics                            |
| extract_lda_words       | Extract top words for topics                        |
| sentiment_vader         | Run VADER analysis                                  |
| sentiment_bert          | Run DistilBERT analysis                             |
| sentiment_distilroberta | Run DistilRoBERTa emotion classification            |
| push_statistics         | Push aggregated stats to XCom                       |
| visualize_pipeline      | Generate charts & reports                           |

### DAG Graph

```
placeholder for DAG graph
```

---

## Scrapy Crawler Overview

Project located at: `airflow/scraper/bbcNews/`

Workflow:

1. Load article URLs from Mongo.links
2. Parse various BBC HTML layouts
3. Extract fields: title, subtitle, authors, paragraphs, topics, images
4. Build item object
5. Pipeline performs:

   * field validation
   * insert into Mongo.NewsSpider
   * deduplication (unique index + custom middleware)

Settings can be adjusted in `settings.py`.

---

## NLP / Sentiment / Topic Modeling

All NLP modules are under:
`airflow/plugins/nlp_tasks/`

### 1. Text Cleaning (`data_preparation.py`)

Includes:

* Lowercasing
* Remove URLs and HTML
* Noise removal & stopword filtering
* POS filtering (keep nouns/adj/adv)
* WordNet lemmatization
* Remove short text (<50 words)

Output: `articles_processed`

---

### 2. LDA Topic Modeling (`topic_modeling.py`)

* Train 12-topic and 32-topic LDA models
* Compute coherence
* Generate pyLDAvis HTML reports
* Assign dominant topic per article

Output: `articles_with_topics`

---

### 3. Sentiment & Emotion Analysis (`sentiment_analysis_*.py`)

| Model                 | Description                     | Collection                     |
| --------------------- | ------------------------------- | ------------------------------ |
| VADER                 | Rule-based polarity scoring     | articles_sentiment_vader       |
| DistilBERT            | Binary sentiment classification | articles_sentiment_bert        |
| DistilRoBERTa Emotion | Multi-emotion classification    | articles_emotion_distilroberta |

---

## Visualization & Reports

All visualization logic is in `stats_visualization.py`.
Output directory:

```
/opt/airflow/dags/output/{YYYY-MM-DD_HHMM}/
  ├─ vader/
  ├─ bert/
  ├─ distilroberta/
  └─ topics/
```

Generated charts include:

* Topic distribution bar charts
* Topic heatmaps (date x topic)
* Topic WordClouds
* Sentiment / emotion pie charts & bar charts
* Daily sentiment trend
* KDE, histogram, boxplot
* Text-length vs sentiment scatter