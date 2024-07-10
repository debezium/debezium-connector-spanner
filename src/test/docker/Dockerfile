FROM confluentinc/cp-kafka-connect-base:latest
ARG projectVersion
USER root
RUN yum remove -y zulu11-ca-jdk-headless && yum remove -y zulu11-ca-jre-headless
RUN yum install -y zulu17-ca-jdk-headless && yum install -y zulu17-ca-jre-headless
USER appuser
COPY target/debezium-connector-spanner-${projectVersion}-plugin/ /usr/share/java/google-debezium-connector-spanner/
COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
