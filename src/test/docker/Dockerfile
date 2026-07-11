FROM mirror.gcr.io/confluentinc/cp-kafka-connect-base
ARG projectVersion
USER root
COPY target/debezium-connector-spanner-${projectVersion}-plugin/debezium-connector-spanner/ /usr/share/java/google-debezium-connector-spanner/
COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
RUN chown -R appuser:appuser /usr/share/java/google-debezium-connector-spanner/
RUN chown -R appuser:appuser /usr/share/prometheus/
USER appuser
