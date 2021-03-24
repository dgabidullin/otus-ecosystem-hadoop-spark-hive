#FROM bde2020/flink-maven-template:1.12.2-hadoop3.2
#
#ENV FLINK_APPLICATION_JAR_NAME app-1.0.0
#ENV FLINK_APPLICATION_MAIN_CLASS net.dag.App

FROM maven:3.6-jdk-8-slim AS builder

COPY ./pom.xml /opt/pom.xml
COPY ./src /opt/src
RUN cd /opt; mvn clean install -Dmaven.test.skip

FROM apache/flink:1.12.1-scala_2.11-java8

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.1/flink-sql-connector-kafka_2.11-1.12.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.12.1/flink-connector-jdbc_2.11-1.12.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.12.1/flink-csv-1.12.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar;

COPY --from=builder /opt/target/app-*.jar /opt/flink/usrlib/flink-app.jar

RUN echo "execution.checkpointing.interval: 10s" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.object-reuse: true" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
