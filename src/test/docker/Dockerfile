FROM confluentinc/cp-kafka-connect-base:7.2.2
COPY target/components/packages/google-debezium-connector-spanner-*.zip /usr/share/confluent-hub-components/google-debezium-connector-spanner.zip
COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
RUN confluent-hub install --no-prompt wepay/kafka-connect-bigquery:latest
RUN confluent-hub install --no-prompt /usr/share/confluent-hub-components/google-debezium-connector-spanner.zip
#CMD ["/bin/bash", "-c", "/etc/confluent/docker/run &; sleep infinity"]
