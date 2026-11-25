FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Kafka source for Structured Streaming, baked in at build time
# (no --packages download on every start, works offline after build).
# Versions must match Spark 3.5.3 / Scala 2.12 (Spark 3.5 ships against kafka-clients 3.4.1).
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN mkdir -p /tmp/spark-work /tmp/spark-events && chown -R spark:spark /tmp/spark-work /tmp/spark-events

USER spark
WORKDIR /opt/jobs
