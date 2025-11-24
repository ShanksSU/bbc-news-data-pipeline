# BBC News Scraper & NLP Pipeline

以 Apache Airflow 為排程與管線協調核心，整合 Scrapy 爬蟲、MongoDB 儲存、NLP（文字前處理、主題模型、情感與情緒分析）、統計圖表輸出，並透過 Docker Compose 一鍵啟動整套自動化新聞資料分析系統。

---

## 目錄

1. [專案總覽](#專案總覽)
2. [系統架構](#系統架構)
3. [技術棧](#技術棧)
4. [目錄結構說明](#目錄結構說明)
5. [環境需求](#環境需求)
6. [如何啟動專案](#如何啟動專案)
7. [Airflow DAG 流程說明](#airflow-dag-流程說明)
8. [Scrapy 爬蟲專案說明](#scrapy-爬蟲專案說明)
9. [NLP / 情緒分析 / 主題模型](#nlp--情緒分析--主題模型)
10. [視覺化輸出與報表](#視覺化輸出與報表)
---

## 專案總覽

本專案旨在建立一條完整、自動化的 BBC News 資料分析管線，能夠週期性執行以下任務：

1. 透過 BBC Sitemap 自動蒐集最新新聞網址
2. 使用 Scrapy 爬蟲抓取 BBC 新聞內容並存入 MongoDB
3. 對新聞內容進行文字前處理（清洗、POS 過濾、lemmatization）
4. 執行多組 LDA 主題模型（12 / 32 topics）
5. 執行三種情緒與情感分析

   * VADER（規則式情感分析）
   * DistilBERT（二分類情感分析）
   * DistilRoBERTa（多情緒分類）
6. 自動生成統計與視覺化圖表（Bar、Pie、Trend、Heatmap、WordCloud 等）
7. 由 Apache Airflow 以 DAG 控制整體流程、錯誤處理、重試策略與排程

整套系統以 Docker Compose 一鍵部署，並具備完整的模組化設計，易於擴展更多新聞來源、模型或統計功能。

---

## 系統架構

以下為整體資料流與模組間的互動架構：

```
我是圖片
```

Airflow 容器透過 Docker 網路連線至：

* mongo（新聞資料庫）
* postgres（Airflow metadata）
* redis（Celery broker）

---

## 技術棧

### 排程與工作流

* Apache Airflow 2.10.x
* Celery Executor
* Redis（Broker）
* PostgreSQL（Airflow Metadata）

### 爬蟲

* Scrapy
* BeautifulSoup / lxml（解析 Sitemap）

### 儲存層

* MongoDB（新聞資料與 NLP 結果）
* Docker volume 持久化

### NLP 模型

* NLTK（stopwords、tokenization、VADER）
* Gensim（LDA 主題模型、Coherence 計算）
* HuggingFace Transformers（DistilBERT / DistilRoBERTa）
* Pandas / NumPy

### 視覺化

* Matplotlib
* Seaborn
* WordCloud
* pyLDAvis（主題模型 HTML 視覺化）

### 容器化

* Docker
* Docker Compose
* 自訂 Airflow Dockerfile（含 NLTK 資料、Scrapy、Transformers）

---

## 目錄結構說明

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
│  │  ├─ bbc_news_dag.py                  # Airflow 主 DAG：抓取 + NLP + 視覺化
│  │  ├─ test_connection_dag.py           # 測試 DAG
│  │  └─ output/                          # 視覺化結果輸出（時間戳為資料夾名）
│  │       └─ YYYY-MM-DD_HHMM/
│  │           ├─ vader/                  # VADER 圖表
│  │           ├─ bert/                   # BERT 圖表
│  │           ├─ distilroberta/          # Emotion 圖表
│  │           └─ topics/                 # 主題模型圖表（heatmap、wordcloud 等）
│  │
│  ├─ logs/                               # Airflow 執行紀錄（scheduler, tasks, dag_processor）
│  │   ├─ dag_id=bbc_news_pipeline/      
│  │   ├─ dag_id=test_connection_dag/
│  │   ├─ dag_processor_manager/
│  │   └─ scheduler/
│  │
│  ├─ models/                              # LDA 模型產物 (model, dict, html)
│  │   ├─ 12_YYYY-MM-DD_HHMM/
│  │   │    ├─ lda_12.model
│  │   │    ├─ lda_12.dict
│  │   │    ├─ lda_12.model.state
│  │   │    ├─ lda_12.model.expElogbeta.npy
│  │   │    ├─ lda_12.model.id2word
│  │   │    └─ lda_vis_12.html
│  │   └─ 32_YYYY-MM-DD_HHMM/
│  │        ├─ lda_32.model
│  │        ├─ lda_32.dict
│  │        ├─ lda_32.model.state
│  │        ├─ lda_32.model.expElogbeta.npy
│  │        ├─ lda_32.model.id2word
│  │        └─ lda_vis_32.html
│  │
│  ├─ plugins/
│  │  └─ nlp_tasks/                        # NLP 任務模組（Airflow plugin）
│  │       ├─ archive_scraper.py           # Sitemap archive 蒐集工具
│  │       ├─ data_preparation.py          # 文字清洗與詞形還原
│  │       ├─ sentiment_analysis.py        # 若有使用（通用 sentiment 模組）
│  │       ├─ sentiment_analysis_bert.py   # BERT 情感分析
│  │       ├─ sentiment_analysis_DistilRoBERTa.py   # 多情緒分類
│  │       ├─ sentiment_analysis_vader.py  # VADER 情感分析
│  │       ├─ stats.py                     # 統計計算（topic / emotion / sentiment）
│  │       ├─ stats_visualization.py       # 全套圖表繪製邏輯
│  │       └─ topic_modeling.py            # LDA 主題模型（訓練、儲存、top words）
│  │
│  └─ scraper/
│      ├─ scrapy.cfg                       # Scrapy 主設定
│      │
│      └─ bbcNews/                         # Scrapy 專案根目錄
│          ├─ items.py                     # 定義 BBC 新聞 item 欄位
│          ├─ middlewares.py               # 去重 middleware、proxy middleware
│          ├─ pipelines.py                 # MongoDB pipeline（插入與重複處理）
│          ├─ settings.py                  # Scrapy 行為設定（headers, delay, retry）
│          │
│          └─ spiders/
│               └─ ArticlesSpider.py       # 主爬蟲，解析 BBC News 文章內容
│
├─ data/
│  └─ mongo/                                # MongoDB 資料目錄（由 Docker volume 掛載）
│       ├─ storage.bson
│       ├─ WiredTiger.turtle
│       ├─ WiredTiger.wt
│       ├─ mongod.lock
│       ├─ sizeStorer.wt
│       ├─ journal/                         # MongoDB WAL 紀錄
│       └─ diagnostic.data/                 # Stateful metrics
│
└─ temp/                                     # 使用者暫存資料夾
```

---

## 環境需求

* Docker
* Docker Compose
* 8GB RAM 以上（若要執行 Transformer 推論建議 12GB+）

---

## 如何啟動專案
### Step 0（可選）：清理既有容器與資料

```bash
docker compose down --volumes --remove-orphans
```

### Step 1：準備 `.env`

專案根目錄下建立：

```
AIRFLOW_UID=0
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### Step 2：啟動容器

可以使用以下方式完整重建 image：

```bash
docker-compose build --no-cache
docker-compose up --build -d
```

或一般啟動方式：

```bash
docker compose up -d --build
```

啟動內容包含：

* Airflow（webserver / scheduler / worker / triggerer）
* MongoDB
* Redis
* PostgreSQL
* Airflow 初始化程序（自動建立管理者帳號）

### Step 3：重新啟動整套服務（若有更新 DAG / Dockerfile）

```bash
docker compose restart
```

### Step 4：存取 Airflow UI

* URL: [http://localhost:8080](http://localhost:8080)
* User: `airflow`
* Password: `airflow`

啟用 `bbc_news_pipeline` DAG 即可。

---

## Airflow DAG 流程說明

主 DAG：`bbc_news_dag.py`

### Task 清單

| Task ID                 | 功能                               |
| ----------------------- | -------------------------------- |
| get_docs_count          | 計算目前 Mongo 中 links 與爬蟲文章數量       |
| collect_urls            | 解析 BBC Sitemap，新增網址至 Mongo.links |
| crawl_news              | 執行 Scrapy，爬取最新文章                 |
| data_preparation        | 文本清洗、POS 過濾、lemmatization        |
| topic_model_32          | 以 32 topics 訓練 LDA               |
| topic_model_12          | 以 12 topics 訓練 LDA               |
| extract_lda_words       | 擷取 LDA 主題字詞                      |
| sentiment_vader         | VADER 情感分析                       |
| sentiment_bert          | DistilBERT 情感分析                  |
| sentiment_distilroberta | DistilRoBERTa 情緒分類               |
| push_statistics         | 推送 NLP 統計數據至 XCom                |
| visualize_pipeline      | 產生完整圖表報表                         |

### DAG 流程圖

```
我是DAG 流程圖
```

---

## Scrapy 爬蟲專案說明

Scrapy 專案位於：`airflow/scraper/bbcNews/`

爬蟲流程：

1. 從 Mongo.links 讀取新聞 URL
2. 解析 BBC 不同版本的 HTML 結構
3. 擷取欄位：標題、副標題、作者、主要段落、話題標籤、圖片
4. 抽取文本並組裝成 item
5. Pipeline 進行：

   * 欄位完整性檢查
   * 寫回 Mongo.NewsSpider
   * 避免重複資料（unique index + Skip Middleware）

Scrapy 參數可於 `settings.py` 調整。

---

## NLP / 情緒分析 / 主題模型

所有 NLP 程式位於：`airflow/plugins/nlp_tasks/`

### 1. 文字清洗（data_preparation.py）

內容包括：

* Lowercase
* 移除 URL、HTML 標記
* 移除雜訊字串與停用詞
* POS 過濾（僅保留名詞/形容詞/副詞）
* WordNet Lemmatization
* 移除短文本（<50 words）

輸出：`articles_processed`

---

### 2. LDA 主題模型（topic_modeling.py）

* 使用 Gensim 訓練主題模型（12 / 32 topics）
* 計算 coherence 分數
* pyLDAvis HTML 輸出
* 每篇文章標記主題（main_topic）

輸出：`articles_with_topics`

---

### 3. 情感分析（sentiment_analysis_*.py）

| 模型                    | 說明                            | Collection                     |
| --------------------- | ----------------------------- | ------------------------------ |
| VADER                 | 規則式 sentiment score（負向/正向/中立） | articles_sentiment_vader       |
| DistilBERT            | HuggingFace 二分類情感分析           | articles_sentiment_bert        |
| DistilRoBERTa Emotion | 多情緒分類（anger、joy、sadness 等）    | articles_emotion_distilroberta |

---

## 視覺化輸出與報表

所有統計圖表由 `stats_visualization.py` 產生，輸出至：

```
/opt/airflow/dags/output/{YYYY-MM-DD_HHMM}/
  ├─ vader/
  ├─ bert/
  └─ distilroberta/
  └─ topics/
```

產生的圖表包含：

* 主題分布 Bar Chart
* 主題 Heatmap（日期 x 主題）
* 主題 WordCloud（每主題）
* 情緒 / 情感分布 Pie、Bar
* 多日情緒趨勢圖（trend）
* KDE、Histogram、Boxplot
* Text-length vs Sentiment Scatter

---