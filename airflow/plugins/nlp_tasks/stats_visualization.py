import os
import numpy as np
from datetime import datetime, timedelta

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import dates as mdates
from wordcloud import WordCloud, STOPWORDS
from sklearn.feature_extraction.text import TfidfVectorizer

# Configuration: Custom Stopwords
NEWS_STOPWORDS = set(STOPWORDS).union({
    "said", "says", "say", "told", "reported", "added", "according", "stated",
    "year", "years", "time", "day", "week", "month", "today", "yesterday", "now",
    "one", "two", "three", "first", "last", "new", "old", "high", "low",
    "people", "man", "woman", "world", "government", "country", "state",
    "bbc", "news", "uk", "image", "source", "caption", "media", "video", "page", "link", "copyright",
    "would", "could", "should", "might", "also", "mr", "mrs", "ms", "dr", "like", "make", "get"
})

def generate_all_stats_figures(**kwargs):
    output_dir = visualize_pipeline_stats()

    ti = kwargs.get("ti")
    if ti:
        ti.xcom_push(key="visualization_output_dir", value=output_dir)

    print(f"[visualization] completed. output directory pushed to xcom: {output_dir}")
    return output_dir

def create_unique_dir(base_dir: str) -> str:
    if not os.path.exists(base_dir):
        return base_dir
    n = 1
    while os.path.exists(f"{base_dir}_{n}"):
        n += 1
    return f"{base_dir}_{n}"

def save_plot(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_pie(series, title, path):
    if series.empty:
        return
    plt.figure(figsize=(7, 7))
    colors = sns.color_palette('pastel')[0:len(series)]
    series.plot(kind="pie", autopct="%1.1f%%", startangle=90, colors=colors)
    plt.title(title, fontsize=14)
    plt.ylabel("")
    save_plot(path)

def plot_bar(series, title, xlabel, ylabel, path):
    if series.empty:
        return
    plt.figure(figsize=(10, 6))
    series.plot(kind="bar", color="skyblue", edgecolor="black")
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    save_plot(path)

def plot_trend_line(df, date_col, value_col, title, ylabel, path):
    if df.empty:
        return
    plt.figure(figsize=(12, 6))
    plt.plot(df[date_col], df[value_col], marker='o', linestyle='-', linewidth=2, markersize=6)
    plt.title(title, fontsize=14)
    plt.xlabel("Date")
    plt.ylabel(ylabel)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    plt.grid(True, linestyle='--', alpha=0.5)
    save_plot(path)

def plot_stacked_bar_trend(df, date_col, cat_col, title, path):
    if df.empty:
        return
    pivot = df.groupby([date_col, cat_col]).size().unstack(fill_value=0)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
    plt.figure(figsize=(12, 6))
    pivot_pct.plot(kind='bar', stacked=True, ax=plt.gca(), width=0.8, alpha=0.9, colormap='Set2')
    
    plt.title(title, fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Percentage (%)")
    plt.legend(title=cat_col, bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    plt.savefig(path, bbox_inches='tight')
    plt.close()

def plot_hist(df, col, title, path):
    if df.empty or col not in df.columns:
        return
    plt.figure(figsize=(10, 6))
    plt.hist(df[col], bins=30, color='lightgreen', edgecolor='black', alpha=0.7)
    plt.title(title, fontsize=14)
    plt.xlabel("Score")
    plt.ylabel("Frequency (Count)")
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    save_plot(path)

def plot_scatter(df, x, y, title, xlabel, ylabel, path):
    if df.empty or x not in df.columns or y not in df.columns:
        return
    plt.figure(figsize=(8, 6))
    plt.scatter(df[x], df[y], alpha=0.4, color='purple')
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, linestyle='--', alpha=0.5)
    save_plot(path)

def plot_wordcloud_simple(words_list, title, path):
    if not words_list:
        return
    text = " ".join(words_list)
    wc = WordCloud(
        width=800, height=400, 
        background_color="white", 
        colormap="viridis",
        stopwords=NEWS_STOPWORDS,
        min_word_length=3
    ).generate(text)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=14)
    save_plot(path)

def plot_wordcloud_freq(freq_dict, title, path):
    if not freq_dict:
        return
    
    clean_freq = {k: v for k, v in freq_dict.items() if k.lower() not in NEWS_STOPWORDS}
    
    wc = WordCloud(
        width=800, height=400, 
        background_color="white", 
        colormap="magma"
    ).generate_from_frequencies(clean_freq)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=14)
    save_plot(path)

# Helper: Date Parsing
def ensure_date_column(df, possible_cols=None):
    if possible_cols is None:
        possible_cols = ["date", "lastmod", "published_at", "time", "created_at"]
    
    target_col = None
    for col in possible_cols:
        if col in df.columns:
            target_col = col
            break
            
    if target_col:
        df["date_parsed"] = pd.to_datetime(df[target_col], errors="coerce")
        df = df.dropna(subset=["date_parsed"])
        df["date"] = df["date_parsed"].dt.date
        return df
    else:
        print(f"[WARN] No date column found among {possible_cols}. Columns present: {df.columns.tolist()}")
        return pd.DataFrame()

def visualize_pipeline_stats(output_root="/opt/airflow/dags/output"):
    print("[visualization] starting pipeline...")

    # Timestamped output directory
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    base_dir = os.path.join(output_root, timestamp)
    output_dir = create_unique_dir(base_dir)
    os.makedirs(output_dir, exist_ok=True)
    print(f"[visualization] output directory = {output_dir}")

    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    print("[visualization] generating trending keywords (48h)...")
    hot_col = "articles_processed"
    if hot_col in db.list_collection_names():
        cursor = db[hot_col].find({}, {"article_clean": 1, "date": 1, "lastmod": 1})
        df_hot = pd.DataFrame(list(cursor))
        
        if not df_hot.empty:
            df_hot = ensure_date_column(df_hot)
            if not df_hot.empty and "date_parsed" in df_hot.columns:
                cutoff_time = datetime.now() - timedelta(hours=48)
                recent_df = df_hot[df_hot["date_parsed"] >= cutoff_time]
                
                print(f"[visualization] Found {len(recent_df)} articles in the last 48 hours.")
                
                if not recent_df.empty:
                    try:
                        # TF-IDF 計算
                        tfidf = TfidfVectorizer(max_features=100, stop_words=list(NEWS_STOPWORDS))
                        tfidf_matrix = tfidf.fit_transform(recent_df["article_clean"].astype(str))
                        feature_names = tfidf.get_feature_names_out()
                        
                        dense = tfidf_matrix.todense()
                        word_scores = dense.sum(axis=0).tolist()[0]
                        keywords_dict = {feature_names[i]: word_scores[i] for i in range(len(feature_names))}
                        
                        trend_folder = os.path.join(output_dir, "trending")
                        os.makedirs(trend_folder, exist_ok=True)
                        
                        plot_wordcloud_freq(
                            keywords_dict,
                            "Trending Keywords (Last 48 Hours)",
                            os.path.join(trend_folder, "cloud_hot_keywords.png")
                        )
                        
                        sorted_keywords = sorted(keywords_dict.items(), key=lambda x: x[1], reverse=True)[:15]
                        top_df = pd.DataFrame(sorted_keywords, columns=["keyword", "score"])
                        plot_bar(
                            top_df.set_index("keyword")["score"],
                            "Top 15 Hot Keywords (48h)",
                            "Keyword", "TF-IDF Score",
                            os.path.join(trend_folder, "bar_hot_keywords.png")
                        )
                    except Exception as e:
                        print(f"[visualization] Keyword extraction failed: {e}")
            else:
                print("[visualization] Valid dates missing for keyword extraction.")
    else:
        print(f"[visualization] Collection {hot_col} not found.")

    sentiment_models = {
        "vader": ("articles_sentiment_vader", "VADER"),
        "bert": ("articles_sentiment_bert", "BERT"),
    }

    for key, (collection, name) in sentiment_models.items():
        folder = os.path.join(output_dir, key)
        os.makedirs(folder, exist_ok=True)

        if collection not in db.list_collection_names():
            print(f"[{name}] collection not found, skipping.")
            continue

        df = pd.DataFrame(list(db[collection].find({})))
        if df.empty: continue

        df = ensure_date_column(df)
        if df.empty:
            print(f"[{name}] No valid dates found, skipping time plots.")
            continue

        if "sentiment_label" in df.columns:
            plot_pie(df["sentiment_label"].value_counts(),
                     f"{name} Overall Sentiment Distribution",
                     os.path.join(folder, "pie_sentiment.png"))
            
            plot_stacked_bar_trend(
                df, "date", "sentiment_label",
                f"{name} Daily Sentiment Proportion",
                os.path.join(folder, "trend_sentiment_stacked.png")
            )

        if "sentiment_score" in df.columns:
            daily_score = df.groupby("date")["sentiment_score"].mean().reset_index()
            plot_trend_line(daily_score, "date", "sentiment_score",
                       f"{name} Daily Average Sentiment Score",
                       "Average Score (-1 to +1)",
                       os.path.join(folder, "trend_score_line.png"))

            plot_hist(df, "sentiment_score",
                      f"{name} Score Distribution",
                      os.path.join(folder, "hist_score.png"))

    emo_col = "articles_emotion_distilroberta"
    emo_folder = os.path.join(output_dir, "distilroberta")
    os.makedirs(emo_folder, exist_ok=True)

    if emo_col in db.list_collection_names():
        df = pd.DataFrame(list(db[emo_col].find({})))
        if not df.empty:
            df = ensure_date_column(df)
            if not df.empty and "emotion_label" in df.columns:
                plot_pie(
                    df["emotion_label"].value_counts(),
                    "Overall Emotion Distribution",
                    os.path.join(emo_folder, "pie_emotion.png")
                )
                plot_bar(
                    df["emotion_label"].value_counts(),
                    "Emotion Counts",
                    "Emotion", "Count",
                    os.path.join(emo_folder, "bar_emotion.png")
                )
                plot_stacked_bar_trend(
                    df, "date", "emotion_label",
                    "Daily Emotion Trends (Proportion)",
                    os.path.join(emo_folder, "trend_emotion_stacked.png")
                )

    target_topic_collections = [
        ("articles_topic_12", "LDA_12_Topics"),
        ("articles_topic_32", "LDA_32_Topics"),
        ("articles_with_topics", "LDA_Topics_Default")
    ]

    for topic_col, model_name in target_topic_collections:
        if topic_col not in db.list_collection_names():
            continue
            
        print(f"[visualization] Processing Topic Model: {model_name}...")
        df = pd.DataFrame(list(db[topic_col].find({})))
        if df.empty: continue

        topic_folder = os.path.join(output_dir, "topics", model_name)
        os.makedirs(topic_folder, exist_ok=True)
        
        df = ensure_date_column(df)

        if "main_topic" in df.columns:
            plot_bar(
                df["main_topic"].value_counts().sort_index(),
                f"{model_name} - Article Count per Topic",
                "Topic ID", "Count",
                os.path.join(topic_folder, "bar_topics.png")
            )

            if not df.empty and "url" in df.columns:
                try:
                    pivot = df.pivot_table(
                        index="date",
                        columns="main_topic",
                        values="url",
                        aggfunc="count",
                        fill_value=0
                    )
                    plt.figure(figsize=(14, 8))
                    sns.heatmap(pivot, cmap="YlOrRd", annot=True, fmt="d", linewidths=.5)
                    plt.title(f"{model_name} - Daily Topic Heatmap", fontsize=14)
                    plt.xlabel("Topic ID")
                    plt.ylabel("Date")
                    save_plot(os.path.join(topic_folder, "heatmap_topics.png"))
                except Exception as e:
                    print(f"[visualization] Topic heatmap error: {e}")

            if "article_clean" in df.columns:
                df["text_len"] = df["article_clean"].apply(lambda t: len(str(t).split()))
                plot_scatter(
                    df, "main_topic", "text_len",
                    "Topic vs. Article Length",
                    "Topic ID", "Word Count",
                    os.path.join(topic_folder, "scatter_topic_length.png")
                )

                # topics wordcloud 1-12
                unique_topics = sorted(df["main_topic"].unique())
                for topic_id in unique_topics[:12]:
                    subset = df[df["main_topic"] == topic_id]
                    if not subset.empty:
                        words = " ".join(subset["article_clean"]).split()
                        top_words = words[:5000]
                        plot_wordcloud_simple(
                            top_words,
                            f"{model_name} - Topic {topic_id}",
                            os.path.join(topic_folder, f"wordcloud_topic_{topic_id}.png")
                        )

    print(f"[visualization] completed. all files saved in: {output_dir}")
    return output_dir