FROM apache/hive:4.0.1

USER root
# Metastore talks to Postgres; the image ships without the JDBC driver.
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgres.jar
# HDFS client settings (replication=1). /opt/hive/conf is on the Hive classpath.
COPY conf/hdfs-site.xml /opt/hive/conf/hdfs-site.xml
USER hive
