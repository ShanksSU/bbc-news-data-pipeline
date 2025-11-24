import os
import numpy as np
from datetime import datetime

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import dates as mdates
from wordcloud import WordCloud

def generate_all_stats_figures(**kwargs):
    # run full visualization and push dir to xcom
    output_dir = visualize_pipeline_stats()

    ti = kwargs.get("ti")
    if ti:
        ti.xcom_push(key="visualization_output_dir", value=output_dir)

    print(f"[visualization] completed. output directory pushed to xcom: {output_dir}")
    return output_dir

# create unique output directory
def create_unique_dir(base_dir: str) -> str:
    if not os.path.exists(base_dir):
        return base_dir
    n = 1
    while os.path.exists(f"{base_dir}_{n}"):
        n += 1
    return f"{base_dir}_{n}"

# generic plot utilities
def save_plot(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_pie(series, title, path):
    # pie chart
    if series.empty:
        return
    plt.figure(figsize=(6, 6))
    series.plot(kind="pie", autopct="%1.1f%%", startangle=90)
    plt.title(title)
    save_plot(path)

def plot_bar(series, title, xlabel, ylabel, path):
    # bar chart
    if series.empty:
        return
    plt.figure(figsize=(8, 5))
    series.plot(kind="bar")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    save_plot(path)

def plot_trend(df, col, title, ylabel, path):
    # time series trend
    if df.empty or col not in df.columns:
        return
    plt.figure(figsize=(12, 5))
    plt.plot(df["date"], df[col], alpha=0.7)
    plt.title(title)
    plt.xlabel("date")
    plt.ylabel(ylabel)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    save_plot(path)

def plot_kde(df, col, title, path):
    # kde distribution
    if df.empty or col not in df.columns:
        return
    plt.figure(figsize=(8, 5))
    sns.kdeplot(df[col], fill=True)
    plt.title(title)
    save_plot(path)

def plot_box(df, col, title, path):
    # boxplot
    if df.empty or col not in df.columns:
        return
    plt.figure(figsize=(6, 4))
    sns.boxplot(x=df[col])
    plt.title(title)
    save_plot(path)

def plot_hist(df, col, title, path):
    # histogram
    if df.empty or col not in df.columns:
        return
    plt.figure(figsize=(8, 5))
    plt.hist(df[col], bins=30, alpha=0.7)
    plt.title(title)
    save_plot(path)

def plot_scatter(df, x, y, title, xlabel, ylabel, path):
    # scatter plot
    if df.empty or x not in df.columns or y not in df.columns:
        return
    plt.figure(figsize=(7, 5))
    plt.scatter(df[x], df[y], alpha=0.3)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    save_plot(path)

# wordcloud for topic words
def plot_wordcloud(words, title, path):
    # wordcloud generation
    if not words:
        return
    wc = WordCloud(
        width=800, height=400, background_color="white"
    ).generate(" ".join(words))

    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    save_plot(path)

# main pipeline: generate full visualization set
def visualize_pipeline_stats(output_root="/opt/airflow/dags/output"):
    print("[visualization] starting…")

    # create output dir with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    base_dir = os.path.join(output_root, timestamp)
    output_dir = create_unique_dir(base_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"[visualization] output directory = {output_dir}")

    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    # sentiment models
    sentiment_models = {
        "vader": ("articles_sentiment_vader", "VADER"),
        "bert": ("articles_sentiment_bert", "BERT"),
    }

    for key, (collection, name) in sentiment_models.items():
        folder = os.path.join(output_dir, key)
        os.makedirs(folder, exist_ok=True)

        if collection not in db.list_collection_names():
            print(f"[{name}] collection not found.")
            continue

        df = pd.DataFrame(list(db[collection].find({})))
        if df.empty:
            print(f"[{name}] empty dataset.")
            continue

        df["date"] = pd.to_datetime(df["date"]).dt.date

        # pie chart
        plot_pie(df["sentiment_label"].value_counts(),
                 f"{name} sentiment distribution",
                 os.path.join(folder, "pie_sentiment.png"))

        # bar chart
        plot_bar(df["sentiment_label"].value_counts(),
                 f"{name} sentiment counts",
                 "label", "count",
                 os.path.join(folder, "bar_sentiment.png"))

        # daily trend
        daily = df.groupby("date")["sentiment_score"].mean().reset_index()
        plot_trend(daily, "sentiment_score",
                   f"{name} daily sentiment score",
                   "score",
                   os.path.join(folder, "trend_score.png"))

        # kde
        plot_kde(df, "sentiment_score",
                 f"{name} sentiment score distribution",
                 os.path.join(folder, "kde_score.png"))

        # boxplot
        plot_box(df, "sentiment_score",
                 f"{name} score boxplot",
                 os.path.join(folder, "box_score.png"))

        # histogram
        plot_hist(df, "sentiment_score",
                  f"{name} score histogram",
                  os.path.join(folder, "hist_score.png"))

        # score vs text length
        df["text_len"] = df["article_clean"].apply(lambda t: len(str(t).split()))
        plot_scatter(df, "text_len", "sentiment_score",
                     f"{name} score vs text length",
                     "text length", "sentiment score",
                     os.path.join(folder, "scatter_score_vs_length.png"))

    # emotion classification
    emo_col = "articles_emotion_distilroberta"
    emo_name = "DistilRoBERTa"
    emo_folder = os.path.join(output_dir, "distilroberta")
    os.makedirs(emo_folder, exist_ok=True)

    if emo_col in db.list_collection_names():
        df = pd.DataFrame(list(db[emo_col].find({})))
        if not df.empty:

            df["date"] = pd.to_datetime(df["date"]).dt.date

            # pie chart
            plot_pie(
                df["emotion_label"].value_counts(),
                "emotion distribution",
                os.path.join(emo_folder, "pie_emotion.png")
            )

            # bar chart
            plot_bar(
                df["emotion_label"].value_counts(),
                "emotion counts",
                "emotion", "count",
                os.path.join(emo_folder, "bar_emotion.png")
            )

            # emotion trend
            trend = (
                df.groupby("date")["emotion_label"]
                .agg(lambda x: x.value_counts().index[0])
                .reset_index()
            )
            trend["emotion_code"] = trend["emotion_label"].astype(
                "category").cat.codes

            plot_trend(
                trend,
                col="emotion_code",
                title="daily dominant emotion trend",
                ylabel="emotion code",
                path=os.path.join(emo_folder, "trend_emotion.png"),
            )

    # topic modeling visualization
    topic_col = "articles_with_topics"
    topic_folder = os.path.join(output_dir, "topics")
    os.makedirs(topic_folder, exist_ok=True)

    if topic_col in db.list_collection_names():
        df = pd.DataFrame(list(db[topic_col].find({})))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date

            # topic count bar chart
            plot_bar(
                df["main_topic"].value_counts().sort_index(),
                "topic distribution",
                "topic", "count",
                os.path.join(topic_folder, "bar_topics.png")
            )

            # topic heatmap
            pivot = df.pivot_table(
                index="date",
                columns="main_topic",
                values="url",
                aggfunc="count",
                fill_value=0
            )

            plt.figure(figsize=(14, 6))
            sns.heatmap(pivot, cmap="Blues")
            plt.title("topic occurrence heatmap")
            save_plot(os.path.join(topic_folder, "heatmap_topics.png"))

            # topic vs word count
            df["text_len"] = df["article_clean"].apply(lambda t: len(str(t).split()))
            plot_scatter(
                df,
                "main_topic", "text_len",
                "topic vs text length",
                "topic", "word count",
                os.path.join(topic_folder, "scatter_topic_length.png")
            )

            # wordcloud per topic
            for topic_id in sorted(df["main_topic"].unique()):
                subset = df[df["main_topic"] == topic_id]
                words = " ".join(subset["article_clean"]).split()
                top_words = words[:5000]

                plot_wordcloud(
                    top_words,
                    f"topic {topic_id} wordcloud",
                    os.path.join(topic_folder, f"wordcloud_topic_{topic_id}.png")
                )

    print(f"[visualization] completed. all files saved in: {output_dir}")
    return output_dir