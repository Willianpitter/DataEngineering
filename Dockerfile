FROM python:3.9-slim


WORKDIR /app

RUN pip install --upgrade pip

COPY ../requirements.txt requirements.txt

COPY app/data_ingestion.py data_ingestion.py


RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2



RUN pip install -r requirements.txt


CMD ["python", "app.py"]

