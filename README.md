![Build Core](https://github.com/debezium/debezium-connector-spanner/actions/workflows/maven.yml/badge.svg)

## Debezium connector spanner

### Prerequisites

need to set google credentials file path \
`export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/service-account-file.json"`

or set connector config parameter `googleApplicationCredentialsFile`

### Tests

Run Unit tests
```
mvn test
```

Run coverage check
```
mvn clean test jacoco:report -P test-coverage
```

- Coverage report for unit tests is available at ${module.path}/target/site/jacoco/index.html
