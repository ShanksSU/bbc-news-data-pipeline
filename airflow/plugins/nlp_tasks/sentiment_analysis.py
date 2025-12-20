import pandas as pd
import torch
from pymongo import MongoClient
from transformers import pipeline
from nltk.sentiment import SentimentIntensityAnalyzer
import gc

class SentimentManager:
    def __init__(self, db_name="bbcnews", input_col="articles_processed"):
        self.client = MongoClient("mongo", 27017)
        self.db = self.client[db_name]
        self.input_col = input_col
        self.df = None

    def load_data(self):
        print(f"[Sentiment] Loading data from {self.input_col}...")
        cursor = self.db[self.input_col].find({}, {"article_clean": 1, "url": 1, "date": 1, "category": 1, "topic": 1})
        self.df = pd.DataFrame(list(cursor))
        
        if self.df.empty:
            print("[Sentiment] No articles found.")
            return False
        
        print(f"[Sentiment] Loaded {len(self.df)} articles.")
        return True

    def save_results(self, output_col_name):
        if self.df is None or self.df.empty:
            return

        save_df = self.df.drop(columns=["_id"], errors="ignore")
        
        print(f"[Sentiment] Saving results to {output_col_name}...")
        self.db[output_col_name].delete_many({})
        self.db[output_col_name].insert_many(save_df.to_dict("records"))
        print(f"[Sentiment] Saved {len(save_df)} records.")

    def run_vader(self, output_col="articles_sentiment_vader"):
        if not self.load_data(): return

        print("[Sentiment] Running VADER...")
        sia = SentimentIntensityAnalyzer()

        self.df["sentiment_score"] = self.df["article_clean"].apply(
            lambda txt: sia.polarity_scores(str(txt))["compound"]
        )

        def label(score):
            if score >= 0.05: return "positive"
            if score <= -0.05: return "negative"
            return "neutral"

        self.df["sentiment_label"] = self.df["sentiment_score"].apply(label)
        self.save_results(output_col)

    def run_bert(self, output_col="articles_sentiment_bert"):
        if not self.load_data(): return
        self._run_transformer(
            model_name="distilbert-base-uncased-finetuned-sst-2-english",
            task="sentiment-analysis",
            label_col="sentiment_label",
            score_col="sentiment_score",
            output_col=output_col
        )

    def run_distilroberta(self, output_col="articles_emotion_distilroberta"):
        if not self.load_data(): return
        self._run_transformer(
            model_name="j-hartmann/emotion-english-distilroberta-base",
            task="text-classification",
            label_col="emotion_label",
            score_col="emotion_score",
            output_col=output_col
        )

    def _run_transformer(self, model_name, task, label_col, score_col, output_col):
        print(f"[Sentiment] Loading Model: {model_name}...")
        device = 0 if torch.cuda.is_available() else -1
        
        classifier = pipeline(
            task,
            model=model_name,
            device=device,
            truncation=True,
            top_k=None if task == "text-classification" else 1
        )

        print(f"[Sentiment] Running Inference on {device}...")
        
        texts = self.df["article_clean"].astype(str).tolist()
        results = classifier(texts, truncation=True) 

        clean_labels = []
        clean_scores = []

        for r in results:
            if isinstance(r, list):
                top = max(r, key=lambda x: x['score'])
                clean_labels.append(top['label'].lower())
                clean_scores.append(float(top['score']))
            else:
                clean_labels.append(r['label'].lower())
                clean_scores.append(float(r['score']))

        self.df[label_col] = clean_labels
        self.df[score_col] = clean_scores

        del classifier
        torch.cuda.empty_cache()
        gc.collect()

        self.save_results(output_col)

def run_vader_wrapper():
    SentimentManager().run_vader()

def run_bert_wrapper():
    SentimentManager().run_bert()

def run_distilroberta_wrapper():
    SentimentManager().run_distilroberta()