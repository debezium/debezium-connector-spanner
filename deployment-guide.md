# Deploying the Debezium Spanner Connector on GKE

## Overview

This guide walks through deploying the Debezium Spanner Connector in a Kafka Connect cluster running on Google Kubernetes Engine (GKE). By the end, you will have a fully operational Kafka Connect environment with Spanner change data capture (CDC) enabled.

---

## Prerequisites

Before beginning, ensure you have the following tools installed and configured:

- [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install)
- [Docker](https://docs.docker.com/get-docker/) with `buildx` support
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Maven](https://maven.apache.org/install.html)
- A [Docker Hub](https://hub.docker.com/) account with push access
- A GCP project with billing enabled
- A GCP Service Account with a downloaded JSON key file
- The [`debezium-connector-spanner`](https://github.com/debezium/debezium-connector-spanner) source repository cloned locally
- The `debezium.connector.ops` repository cloned locally

---

## Part 1: Google Cloud Spanner Setup

### 1.1 Create a Spanner Instance

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Spanner** in the search bar and open the service.
2. Click **Create Instance** and fill in the following:

   | Field    | Value                                                    |
   |----------|----------------------------------------------------------|
   | Edition  | `Enterprise-Plus` *(required for geo-partitioning)*      |
   | Name     | `<INSTANCE_NAME>`                                        |
   | ID       | `<INSTANCE_ID>`                                          |
   | Region   | `Multi-Region nam10` *(or another US multi-region)*      |
   | Nodes    | `1`                                                      |

3. Click **Create**.

### 1.2 Grant Service Account Permissions

1. In the Spanner instance list, check the checkbox next to your new instance.
2. Click **Permissions** in the top action bar.
3. Add your GCP Service Account with the role **Cloud Spanner Admin**.

---

## Part 2: Build and Publish the Docker Image

### 2.1 Set Up Docker Hub

1. Create a [Docker Hub](https://hub.docker.com/) account if you do not already have one.
2. Log in:
   ```bash
   docker login
   ```
3. Generate a Docker Hub **Access Token** with Read & Write permissions:
   - Go to **Account Settings → Security → New Access Token**
   - Save the token securely; you will need it in later steps.

### 2.2 Replace test/docker/DockerFile with the following
  FROM mirror.gcr.io/confluentinc/cp-kafka-connect-base
  ARG projectVersion
  USER root
  COPY target/debezium-connector-spanner-${projectVersion}-plugin/debezium-connector-spanner/ /usr/share/java/google-debezium-connector-spanner/
  COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
  COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
  RUN chown -R appuser:appuser /usr/share/java/google-debezium-connector-spanner/
  RUN chown -R appuser:appuser /usr/share/Prometheus/
  USER appuser

### 2.3 Build the Connector JAR

From the root of the `debezium-connector-spanner` repository, run:

```bash
mvn clean package \
  -Dmaven.test.skip=true \
  -Ppack-local-changes \
  -Ddocker.skip=true
```

### 2.4 Build and Push the Docker Image

```bash
docker buildx build \
  --platform linux/amd64 \
  --build-arg projectVersion=3.6.0.Final \
  -f ./src/test/docker/Dockerfile \
  -t <DOCKER_HUB_USERNAME>/kafka-spanner-connector:<DOCKER_TAG> \
  --push .
```

> Replace `<DOCKER_HUB_USERNAME>` and `<DOCKER_TAG>` with your Docker Hub username and a tag of your choosing (e.g., `1.0.0`).

---

## Part 3: GKE Cluster Setup with Terraform

### 3.1 Configure Terraform Variables

Open `terraform.tfvars` in the root of the `debezium.connector.ops` repository and set the following values:

```hcl
project           = "<GCP_PROJECT_ID>"
region            = "us-central1"
location          = "us-central1-a"
instance_type     = "e2-highmem-16"
node_count        = "18"
gcp_auth_file     = "<SERVICE_ACCOUNT_KEY_RELATIVE_PATH>"
app_name          = "spanner-connector"
registry_username = "<DOCKER_HUB_USERNAME>"
registry_password = "<DOCKER_HUB_TOKEN>"
registry_email    = "<DOCKER_HUB_EMAIL>"
registry_server   = "docker.io"
```

> `node_count` and `replica_count` (in `values.yaml`) should be sized to your workload. The values above are a starting point for high-throughput scenarios.

### 3.2 Configure the Kafka Connect Helm Values

Open `connector/values.yaml` and update the image fields to point to your Docker Hub image:

```yaml
image: <DOCKER_HUB_USERNAME>/kafka-spanner-connector
imageTag: "<DOCKER_TAG>"
```

The remaining configuration in `values.yaml` is provided below as a reference. Review the highlighted sections before deploying:

<details>
<summary>Full <code>connector/values.yaml</code> reference</summary>

```yaml
replicaCount: 36

image: <DOCKER_HUB_USERNAME>/kafka-spanner-connector
imageTag: "<DOCKER_TAG>"

imagePullPolicy: Always

imagePullSecrets:
  - name: kafka-connect

servicePort: 8083

configurationOverrides:
  "plugin.path": "/usr/share/java,/usr/share/confluent-hub-components/,/opt/kafka/spanner/"
  "key.converter": "org.apache.kafka.connect.json.JsonConverter"
  "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  "config.storage.replication.factor": "3"
  "offset.storage.replication.factor": "3"
  "status.storage.replication.factor": "3"
  "config.storage.topic": "_kafka-connect-configs"
  "offset.storage.topic": "_kafka-connect-offsets"
  "status.storage.topic": "_kafka-connect-status"
  "offset.flush.interval.ms": "200"
  "task.shutdown.graceful.timeout.ms": "60000"
  "bootstrap.servers": kafka-cp-kafka-headless:9092
  "rest.advertised.port": "8083"
  "group.id": "kafka-new-group-reader"
  "key.converter.schema.registry.url": "http://kafka-cp-schema-registry:8081"
  "value.converter.schema.registry.url": "http://kafka-cp-schema-registry:8081"
  "log4j.rootLogger": "INFO"
  "log4j.logger.org.apache.kafka.connect.runtime.res": "WARN"
  "log4j.logger.org.reflections": "ERROR"

heapOptions: '-Xms10g -Xmx10g -XX:+UseG1GC -Xlog:gc'

customEnv:
  JAVA_OPTS: >-
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Dcom.sun.management.jmxremote.local.only=false
    -Dcom.sun.management.jmxremote.port=5555
    -Dcom.sun.management.jmxremote.rmi.port=5555
    -Djava.rmi.server.hostname=$(POD_IP)

resources:
  limits:
    cpu: 6000m
    memory: 20Gi
  requests:
    cpu: 4000m
    memory: 20Gi

jmx:
  port: 5555

prometheus:
  jmx:
    enabled: true
    image: solsson/kafka-prometheus-jmx-exporter@sha256
    imageTag: 6f82e2b0464f50da8104acd7363fb9b995001ddff77d248379f8788e78946143
    imagePullPolicy: IfNotPresent
    port: 5556
    resources:
      limits:
        cpu: 500m
        memory: 512Mi
      requests:
        cpu: 100m
        memory: 256Mi

livenessProbe:
  httpGet:
    path: /connectors
    port: 8083
  initialDelaySeconds: 90
  periodSeconds: 15
  failureThreshold: 100

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: /metrics
  prometheus.io/port: "5556"
```

</details>

### 3.3 Deploy the Infrastructure

From the root of the `debezium.connector.ops` repository, initialize and apply Terraform:

```bash
terraform init
terraform apply
```

> This process typically takes **30 or more minutes**. If deployment stalls significantly longer, consider adjusting `node_count`, `instance_type`, or `replicaCount` in `values.yaml`.

---

## Part 4: Enable Authorized Networks on the GKE Cluster

Once Terraform apply has finished, allow your IP address to reach the cluster's control plane:

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Kubernetes Engine**.
2. Click on the **spanner-connector** cluster.
3. Click **Details**, then scroll to **Control Plane Networking** and click the pencil (edit) icon.
4. Under **Access using IPv4 addresses**, check the box for **Enable Authorized Networks**.
5. Add at least one authorized network using a CIDR range that includes the address from which you will be accessing the ports (e.g., `<YOUR_IP_ADDRESS>/32`).
6. Click **Save changes**.

---

## Part 5: Expose Services in GKE

Once Terraform completes, you need to expose four workloads as LoadBalancer services via the GCP Console:

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Kubernetes Engine**.
2. In the left sidebar, click **Workloads**.
3. Filter workloads by the label tag `spanner-connector`.
4. For each of the following workloads, click the workload name → **Actions** → **Expose**, enter the port in both fields, leave the type as **Load Balancer**, and confirm:

   | Workload                            | Port |
   |-------------------------------------|------|
   | `akhq`                              | 8080 |
   | `grafana`                           | 3000 |
   | `kafka-connect-cp-kafka-connect`    | 8083 |
   | `prometheus-server`                 | 9000 |

---

## Part 6: Configure Local Access via Port Forwarding

### 6.1 Fetch Cluster Credentials

```bash
gcloud container clusters get-credentials spanner-connector --zone us-central1-a
```

### 6.2 Start Port Forwards

Run the following commands to forward each service to your local machine:

```bash
kubectl port-forward service/kafka-connect-cp-kafka-connect -n default 8083:8083 &
kubectl port-forward service/grafana -n default 3000:80 &
kubectl port-forward service/akhq -n default 8080:80 &
kubectl port-forward service/prometheus-server -n default 9000:80 &
```

The services will now be accessible at:

| Service         | Local URL                     |
|-----------------|-------------------------------|
| Kafka Connect   | http://localhost:8083         |
| Grafana         | http://localhost:3000         |
| AKHQ            | http://localhost:8080         |
| Prometheus      | http://localhost:9000         |

---

## Part 7: Create the Spanner Database and Change Stream

In a new terminal, run:

```bash
gcloud spanner databases create load-test \
  --instance=spanner-kafka-connector

gcloud spanner databases ddl update load-test \
  --instance=spanner-kafka-connector \
  --ddl="CREATE TABLE BenchmarkUsers (
    UserId INT64 NOT NULL,
    UserName STRING(MAX)
  ) PRIMARY KEY (UserId);
  CREATE CHANGE STREAM mycs FOR BenchmarkUsers;"
```

This creates a `load-test` database with a `BenchmarkUsers` table and enables a Change Stream (`mycs`) on it, which the connector will consume.

---

## Part 8: Register the Connector

### 7.1 Configure `source.json`

Create a `source.json` file (e.g., in the `debezium.connector.ops/gcp-k8s-helm` directory) with the connector configuration. Paste the full contents of your GCP Service Account JSON key into `gcp.spanner.credentials.json` as an escaped string:

```json
{
  "name": "cdc-spanner-connector",
  "config": {
    "connector.class": "io.debezium.connector.spanner.SpannerConnector",
    "gcp.spanner.change.stream": "mycs",
    "gcp.spanner.project.id": "<GCP_PROJECT_ID>",
    "gcp.spanner.instance.id": "spanner-kafka-connector",
    "gcp.spanner.database.id": "load-test",
    "gcp.spanner.low-watermark.enabled": "true",
    "gcp.spanner.low-watermark.update-period.ms": "1000",
    "tasks.max": "20",
    "connector.spanner.sync.kafka.bootstrap.servers": "kafka-cp-kafka:9092",
    "connector.spanner.sync.publisher.wait.timeout": "5000",
    "gcp.spanner.stream.event.queue.capacity": "2000000",
    "topic.creation.default.partitions": "10",
    "topic.creation.default.replication.factor": "1",
    "max.queue.size": "2000000",
    "connector.spanner.max.missed.heartbeats": "600",
    "heartbeat.interval.ms": "1000",
    "gcp.spanner.credentials.json": "{\"type\": \"service_account\", \"project_id\": \"<GCP_PROJECT_ID>\", \"private_key_id\": \"<SERVICE_ACCOUNT_PRIVATE_KEY_ID>\", \"private_key\": \"<SERVICE_ACCOUNT_PRIVATE_KEY>\", \"client_email\": \"<SERVICE_ACCOUNT_EMAIL>\", \"client_id\": \"<SERVICE_ACCOUNT_CLIENT_ID>\", \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\", \"token_uri\": \"https://oauth2.googleapis.com/token\", \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\", \"client_x509_cert_url\": \"<SERVICE_ACCOUNT_CERT_URL>\", \"universe_domain\": \"googleapis.com\"}"
  }
}
```

> The entire service account JSON key must be inlined as a single escaped string value for `gcp.spanner.credentials.json` — double quotes inside it must be escaped with `\"`. Double-check the JSON is valid (e.g., with `jq . source.json`) before submitting it, as a single unescaped quote or missing field will cause the request in the next step to fail.

### 7.2 Submit the Connector Configuration

From the same terminal directory as `source.json`, POST it to the Kafka Connect REST API:

```bash
curl -i -X POST \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d @source.json
```

A `201 Created` response indicates the connector has been registered and will begin streaming changes from Spanner into Kafka.

---

## Appendix: Placeholder Reference

| Placeholder                           | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `<INSTANCE_NAME>`                     | Display name for your Spanner instance              |
| `<INSTANCE_ID>`                       | Unique ID for your Spanner instance                 |
| `<GCP_PROJECT_ID>`                    | Your GCP project ID                                 |
| `<SERVICE_ACCOUNT_KEY_RELATIVE_PATH>` | Relative path to your GCP service account JSON key  |
| `<DOCKER_HUB_USERNAME>`               | Your Docker Hub username                            |
| `<DOCKER_HUB_TOKEN>`                  | Your Docker Hub access token                        |
| `<DOCKER_HUB_EMAIL>`                  | Your Docker Hub account email                       |
| `<DOCKER_TAG>`                        | Tag for your connector Docker image                 |
| `<SERVICE_ACCOUNT_PRIVATE_KEY_ID>`    | `private_key_id` field from your service account key |
| `<SERVICE_ACCOUNT_PRIVATE_KEY>`       | `private_key` field from your service account key   |
| `<SERVICE_ACCOUNT_EMAIL>`             | `client_email` field from your service account key  |
| `<SERVICE_ACCOUNT_CLIENT_ID>`         | `client_id` field from your service account key     |
| `<SERVICE_ACCOUNT_CERT_URL>`          | `client_x509_cert_url` field from your service account key |
| `<YOUR_IP_ADDRESS>`                   | The public IP address (or CIDR range) you'll connect from  |
