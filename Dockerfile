FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/airflow/requirements.txt

# 仍然用 airflow user 裝 pip（官方建議也偏向這樣）
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-openlineage && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

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

ENV NLTK_DATA=/opt/airflow/nltk_data
ENV TWISTED_REACTOR=twisted.internet.asyncioreactor.AsyncioSelectorReactor
WORKDIR /opt/airflow

USER root
RUN set -eux; \
    PY="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"; \
    SITE="$(python -c 'import site; print(site.getsitepackages()[0])')"; \
    echo "/home/airflow/.local/lib/python${PY}/site-packages" > "${SITE}/airflow_user_site.pth"; \
    ln -sf /home/airflow/.local/bin/airflow /usr/local/bin/airflow; \
    ln -sf /home/airflow/.local/bin/celery  /usr/local/bin/celery || true