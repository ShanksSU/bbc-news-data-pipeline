FROM apache/airflow:2.10.5

USER root

# Install only required system packages (no unnecessary build tools)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Remove old openlineage provider if present
RUN pip uninstall -y apache-airflow-providers-openlineage || true

USER airflow

# Install Python dependencies + NLTK resources
RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow-providers-openlineage>=1.8.0" && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt && \
    python -m nltk.downloader \
        stopwords \
        punkt \
        punkt_tab \
        averaged_perceptron_tagger \
        averaged_perceptron_tagger_eng \
        wordnet \
        omw-1.4 \
        vader_lexicon \
        universal_tagset

# Scrapy asyncio reactor
ENV TWISTED_REACTOR=twisted.internet.asyncioreactor.AsyncioSelectorReactor

WORKDIR /opt/airflow