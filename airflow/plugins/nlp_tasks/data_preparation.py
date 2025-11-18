from __future__ import annotations

import re
import string
from typing import Dict, Optional, List

import pandas as pd
from pymongo import MongoClient
import nltk

def load_data(counts: Optional[Dict] = None) -> pd.DataFrame:
    # load latest articles from mongodb
    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    articles = list(
        db.NewsSpider.find().sort("date", -1).limit(1000)
    )
    df = pd.DataFrame(articles)
    return df

def export_data(df: pd.DataFrame):
    # export processed data back to mongodb
    if df.empty:
        print("no data to export.")
        return

    client = MongoClient("mongo", 27017)
    db = client.bbcnews

    # overwrite old cleaned data
    db["articles_processed"].delete_many({})
    db["articles_processed"].insert_many(df.to_dict("records"))

    print(f"[DataPrep] exported {len(df)} processed articles.")

# text cleaning logic
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""

    # basic cleanup
    text = text.lower().strip()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"[^ 0-9a-z]", " ", text)
    text = re.sub(r"\b(\d+\d)\b", "", text)
    text = re.sub(r"http|https|www", "", text)
    text = re.sub(r"\b[a-z]\b", "", text)
    text = re.sub(r" +", " ", text)

    # remove punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))

    # stopwords
    stop_words = set(nltk.corpus.stopwords.words("english"))
    stop_words.update(["from", "re", "also"])

    # keep selected pos classes
    keep_pos = {
        "NN", "NNS", "NNP", "NNPS",  # nouns
        "JJ", "JJR", "JJS",          # adjectives
        "RB", "RBR", "RBS",          # adverbs
    }

    words = [
        w for w, pos in nltk.pos_tag(text.split())
        if len(w) > 2 and w not in stop_words and pos in keep_pos
    ]

    return " ".join(words)

# apply wordnet lemmatization
def lemmatize_text(text: str, lemmatizer) -> str:
    # lemmatize tokens using pos tags
    if not isinstance(text, str):
        return ""

    lemmas: List[str] = []
    tag_map = {
        "J": nltk.corpus.wordnet.ADJ,
        "N": nltk.corpus.wordnet.NOUN,
        "V": nltk.corpus.wordnet.VERB,
        "R": nltk.corpus.wordnet.ADV,
    }

    tokens = nltk.word_tokenize(text)
    for token in tokens:
        tag = nltk.pos_tag([token])[0][1][0].upper()
        wn_pos = tag_map.get(tag, nltk.corpus.wordnet.NOUN)
        lemmas.append(lemmatizer.lemmatize(token, wn_pos))

    return " ".join(lemmas)

# main preprocessing pipeline
def process(counts: Optional[Dict] = None, **kwargs):
    df = load_data(counts)
    if df.empty:
        print("⚠ no articles found. skipping.")
        return

    print(f"[DataPrep] loaded {len(df)} raw articles.")

    # ensure required columns
    for col in ("text", "date", "title"):
        if col not in df:
            df[col] = ""

    # remove missing text
    df = df.dropna(subset=["text"])
    df["n_words"] = df["text"].apply(lambda x: len(str(x).split()))
    df = df[df["n_words"] > 50]  # remove very short articles

    print("[DataPrep] cleaning text...")
    df["article_clean"] = df["text"].apply(clean_text)

    print("[DataPrep] lemmatizing text...")
    lemmatizer = nltk.stem.WordNetLemmatizer()
    df["article_clean"] = df["article_clean"].apply(
        lambda x: lemmatize_text(x, lemmatizer)
    )

    df["n_words_clean"] = df["article_clean"].apply(lambda x: len(x.split()))

    # remove unused fields
    drop_cols = ["images", "topic_name", "topic_url", "link", "authors", "_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df], errors="ignore")

    # convert date format
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.reset_index(drop=True)

    print("[DataPrep] preprocessing completed successfully.")
    print(df.head())

    export_data(df)
    print("[DataPrep] pipeline finished.")
