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

### Integration tests

Run the full IT suite against the local Spanner emulator (default, no real GCP project needed):
```
mvn clean verify
```

Run the full IT suite with tests annotated `@RealSpannerCompatible` (e.g. `MutableKeyRangeIT`)
executed against a real Cloud Spanner instance instead of the emulator, while every other IT class
still runs against the emulator in the same command:
```
mvn clean verify \
  -Dspanner.test.real=true \
  -Dgcp.spanner.project.id=YOUR_PROJECT \
  -Dgcp.spanner.instance.id=YOUR_INSTANCE \
  -Dgcp.spanner.credentials.path=/path/to/key.json   # or -Dgcp.spanner.credentials.json=<inline JSON>
```

Set `-Dspanner.test.real=false` (or omit the property) to keep `@RealSpannerCompatible` tests on the
emulator as well — this is the preferable way to run the suite once the emulator supports
`MUTABLE_KEY_RANGE` change streams; until then those tests are reported as skipped (not failed) when
run without `-Dspanner.test.real=true`.
