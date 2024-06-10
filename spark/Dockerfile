FROM bitnami/spark:latest

USER root

COPY .. /app/.

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["spark-submit", "--jars", "jars/*", "src/kmeans.py"]