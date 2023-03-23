/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import java.time.Duration;
import java.util.UUID;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.stream.SpannerChangeStream;
import io.debezium.connector.spanner.db.stream.SpannerChangeStreamService;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

/** Factory for {@code SpannerChangeStream} */
public class SpannerChangeStreamFactory {

    private static final String JOB_NAME = "SpannerChangeStream_Kafka";

    private final DaoFactory daoFactory;
    private final MetricsEventPublisher metricsEventPublisher;
    private final String connectorName;
<<<<<<< HEAD
    private final Dialect dialect;

    public SpannerChangeStreamFactory(
                                      DaoFactory daoFactory, MetricsEventPublisher metricsEventPublisher, String connectorName,
                                      Dialect dialect) {
=======
    private final String taskUid;

    public SpannerChangeStreamFactory(
                                      String taskUid, DaoFactory daoFactory, MetricsEventPublisher metricsEventPublisher, String connectorName) {
        this.taskUid = taskUid;
>>>>>>> 75d0fe2 (DBZ-6227 Reduce CPU usage and ensure critical logs have task Uid)
        this.daoFactory = daoFactory;
        this.metricsEventPublisher = metricsEventPublisher;
        this.connectorName = connectorName;
        this.dialect = dialect;
    }

    public SpannerChangeStream getStream(
                                         String changeStreamName, Duration heartbeatMillis, int maxMissedHeartbeats) {

        ChangeStreamDao changeStreamDao = daoFactory.getStreamDao(
                changeStreamName,
                Options.RpcPriority.MEDIUM,
                JOB_NAME + "_" + connectorName + "_" + UUID.randomUUID());

        ChangeStreamRecordMapper changeStreamRecordMapper = new ChangeStreamRecordMapper(dialect);

        SpannerChangeStreamService streamService = new SpannerChangeStreamService(
                taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher);

        return new SpannerChangeStream(
                streamService, metricsEventPublisher, heartbeatMillis, maxMissedHeartbeats);
    }
}
