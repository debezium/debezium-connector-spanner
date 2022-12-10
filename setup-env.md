# Setup a local environment for Debezium connector spanner

## For Mac
Docker (docker compose)
```bash
$ brew install docker 
$ brew install docker-compose
```
###  other tools we need git, jq, httpie, direv 

```bash 
$ brew install httpie
$ brew install direv
$ brew install git
$ brew install jq
```
### Sdkman install 
```bash 
$ curl -s "https://get.sdkman.io" | bash
# Follow the instructions on-screen to complete installation.
# Next, open a new terminal or enter:
$ source "$HOME/.sdkman/bin/sdkman-init.sh"
$ sdk version
```


## For Ubuntu
### Docker (docker compose)
```bash
$ sudo apt update 
$ sudo apt install docker-ce docker-ce-cli containerd.io docker-compose-plugin
```
###  other tools we need git, jq, httpie, sdkman, direvent, unzip, zip 
```bash
$ sudo apt install git jq httpie direvent unzip zip
```
### Sdkman install 
```bash 
$ curl -s "https://get.sdkman.io" | bash
# Follow the instructions on-screen to complete installation.
# Next, open a new terminal or enter:
$ source "$HOME/.sdkman/bin/sdkman-init.sh"
$ sdk version
```

###  Sdkman is a tool to manage parallel versions of multiple SDKs
Commands to install:

### Maven (sdkman)
```bash
$ sdk install mvnd
```
### Java (sdkman)
```bash
$ sdk install java 11.0.16-tem
```

## IDE setup (Intellij Idea)
 install plugitns for IDE
### plugins:
```
direnv
sonarlint
```

## Environment variables

### Local setup steps

Add

```text
# Local
127.0.0.1 zookeeper
127.0.0.1 schema-registry
127.0.0.1 kafka
127.0.0.1 rest-proxy
127.0.0.1 control-center
```



to `/etc/hosts` on `local` host

`.envrc`: 
```console
## Examples  
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/work/spanner/utility-operand-danyl.json”
export SPANNER_CONNECTOR_PROJECT_DIR="$HOME/work/spanner/debezium.connector.spanner”
export JAVA_DEBUG_PORT="*:5005”
export KAFKA_DEBUG=y # "y" - debug enabled, "n" - not enabled
export DEBUG_SUSPEND_FLAG=n # "y" - wait until debugger connected, "n" - do not wait
```

`.sdkmanrc`:
```
# Enable auto-env through the sdkman_auto_env config
# Add key=value pairs of SDKs to use below
java=11.0.16-tem
```


##  Run kafka in docker-compose
```bash
$ docker-compose up -d 
$ docker-compose ps
```

## Maven build and test commands
```bash
# build binaries
$ mvn -B clean install 
# integration tests
$ mvn -pl debezium-connector-spanner-integration-tests verify -P integration-tests

$ mvn test --batch-mode -Dmaven.test.failure.ignore=true
```

## Spanner source connector config commands:
```bash
#>
http POST localhost:8083/connectors < ./spanner-source.json
http GET localhost:8083/connectors/my_spanner_connector
http DELETE localhost:8083/connectors/my_spanner_connector
```
Wait for 30-60 sec. Check connector is working:
```bash
#>
http kafka-connect:8083/connectors/my_spanner_connector/status
```
## Spanner source connector config example: spanner-source.json
```text
{
    "name": "my_spanner_connector",
    "config": {
        "connector.class": "io.debezium.connector.spanner.SpannerConnector",
        "heartbeat.interval.ms": "300000",
        "gcp.spanner.change.stream": "TestStream",
        "gcp.spanner.project.id": "boxwood-weaver-353315",
        "gcp.spanner.instance.id": "kafka-connector",
        "gcp.spanner.database.id": "stage-1",
        "gcp.spanner.low-watermark.enabled": true,
        "gcp.spanner.low-watermark.update-period.ms": 1000
    }
}
```

## Misc links
- [Docker install](https://docs.docker.com/engine/install/)
- [Connect REST Interface](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Create connector](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-connector/)
- [Kafka REST](https://docs.confluent.io/platform/current/kafka-rest/api.html#rest-api-usage-examples)
- [Confluent Hub](https://www.confluent.io/hub/)
- [httpie](https://httpie.io/docs/cli/http-method)
- [Postgres Debezium does not publish the previous state of a record](https://stackoverflow.com/questions/59799503/postgres-debezium-does-not-publish-the-previous-state-of-a-record)
- [Stackoverflow how to run kafka-connector on debug mode](https://stackoverflow.com/questions/45717658/what-is-a-simple-effective-way-to-debug-custom-kafka-connectors)
