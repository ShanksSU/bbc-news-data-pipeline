FROM apache/airflow:2.10.5

USER root

# install system packages as root (allowed)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/airflow/requirements.txt

# switch to airflow user BEFORE pip install
USER airflow

# install Python deps as airflow user
RUN pip install --no-cache-dir apache-airflow-providers-openlineage && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# download nltk data as airflow user
RUN python -m nltk.downloader -d /opt/airflow/nltk_data \
    stopwords \
    punkt \
    punkt_tab \
    averaged_perceptron_tagger \
    averaged_perceptron_tagger_eng \
    wordnet \
    omw-1.4 \
    vader_lexicon \
    universal_tagset

# set nltk data path
ENV NLTK_DATA=/opt/airflow/nltk_data
ENV TWISTED_REACTOR=twisted.internet.asyncioreactor.AsyncioSelectorReactor

WORKDIR /opt/airflow
