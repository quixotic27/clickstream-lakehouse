# Spark 3.5 + the extra jars we need: Iceberg (table format), AWS bundle (S3 access), Kafka source
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG=1.6.1
ARG SPARK=3.5.3

USER root
RUN pip install --no-cache-dir pytest==8.3.3
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK}/spark-sql-kafka-0-10_2.12-${SPARK}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK}/spark-token-provider-kafka-0-10_2.12-${SPARK}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# checkpoint folder owned by the spark user (a new Docker volume inherits this ownership)
RUN mkdir -p /opt/checkpoints && chown -R spark:spark /opt/checkpoints

COPY --chown=spark:spark . /opt/app
ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
USER spark
WORKDIR /opt/app