/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.coordinator.impl;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.io.Closeable;
import java.util.HashMap;

/***
 * Describes the working status of the {@link TxnLogBufferedWriter}, helps users tune the thresholds of
 * {@link TxnLogBufferedWriter} for best performance.
 * Note-1: When batch feature is turned off, no data is logged at this. In this scenario，users can see the
 *    {@link org.apache.bookkeeper.mledger.ManagedLedgerMXBean}.
 * Note-2: Even if enable batch feature, if a single record is too big, it still directly write to Bookie without batch,
 *    property {@link #batchFlushTriggeringByLargeSingleDataMetric} can indicate this case. But this case will not
 *    affect
 *    other metrics, because it would obscure the real situation. E.g. there has two record:
 *    [{recordsCount=512, triggerByMaxRecordCount}, {recordCount=1, triggerByTooLarge}], we should not tell the users
 *    that the average batch records is 256, if the users knows that there are only 256 records per batch, then users
 *    will try to increase "TxnLogBufferedWriter#batchedWriteMaxDelayInMillis" so that there is more data per
 *    batch to improve throughput, but that does not work.
 */
public class TxnLogBufferedWriterMetricsStats implements Closeable {

    /**
     * Key is the name we used to create {@link TxnLogBufferedWriterMetricsStats}, and now there are two kinds:
     * ["pulsar_txn_tc_log", "pulsar_txn_tc_batched_log"]. There can be multiple labels in each
     * {@link TxnLogBufferedWriterMetricsStats}, such as The Transaction Coordinator using coordinatorId as label and
     * The Transaction Pending Ack Store using subscriptionName as label.
     */
    private static final HashMap<String, Collector> COLLECTOR_CACHE = new HashMap<>();

    static final double[] RECORD_COUNT_PER_ENTRY_BUCKETS = {10, 50, 100, 200, 500, 1_000};

    static final double[] BYTES_SIZE_PER_ENTRY_BUCKETS = {128, 512, 1_024, 2_048, 4_096, 16_384,
            102_400, 1_232_896};

    static final double[] MAX_DELAY_TIME_BUCKETS = {1, 5, 10};

    private final String metricsPrefix;

    private final String[] labelNames;

    private final String[] labelValues;

    /** Count of records in per transaction log batch. **/
    private final Histogram recordsPerBatchMetric;
    private final Histogram.Child recordsPerBatchHistogram;

    /** Bytes size per transaction log batch. **/
    private final Histogram batchSizeBytesMetric;
    private final Histogram.Child batchSizeBytesHistogram;

    /** The time of the oldest transaction log spent in the buffer before being sent. **/
    private final Histogram oldestRecordInBatchDelayTimeSecondsMetric;
    private final Histogram.Child oldestRecordInBatchDelayTimeSecondsHistogram;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxRecords". **/
    private final Counter batchFlushTriggeringByMaxRecordsMetric;
    private final Counter.Child batchFlushTriggeringByMaxRecordsCounter;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxSize". **/
    private final Counter batchFlushTriggeringByMaxSizeMetric;
    private final Counter.Child batchFlushTriggeringByMaxSizeCounter;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxDelayInMillis". **/
    private final Counter batchFlushTriggeringByMaxDelayMetric;
    private final Counter.Child batchFlushTriggeringByMaxDelayCounter;

    /**
     * If {@link TxnLogBufferedWriter#asyncAddData(Object, TxnLogBufferedWriter.AddDataCallback, Object)} accept a
     * request that param-data is too large (larger than "batchedWriteMaxSize"), then two flushes are executed:
     *    1. Write the data cached in the queue to BK.
     *    2. Direct write the large data to BK.
     * This ensures the sequential nature of multiple writes to BK.
     */
    private final Counter batchFlushTriggeringByLargeSingleDataMetric;
    private final Counter.Child batchFlushTriggeringByLargeSingleDataCounter;

    public void close() {
        recordsPerBatchMetric.remove(labelValues);
        batchSizeBytesMetric.remove(labelValues);
        oldestRecordInBatchDelayTimeSecondsMetric.remove(labelValues);
        batchFlushTriggeringByMaxRecordsMetric.remove(labelValues);
        batchFlushTriggeringByMaxSizeMetric.remove(labelValues);
        batchFlushTriggeringByMaxDelayMetric.remove(labelValues);
        batchFlushTriggeringByLargeSingleDataMetric.remove(labelValues);
    }

    public TxnLogBufferedWriterMetricsStats(String metricsPrefix, String[] labelNames, String[] labelValues,
                                            CollectorRegistry registry) {
        this.metricsPrefix = metricsPrefix;
        this.labelNames = labelNames.clone();
        this.labelValues = labelValues.clone();

        String recordsPerBatchMetricName =
                String.format("%s_batched_log_records_count_per_entry", metricsPrefix);
        recordsPerBatchMetric = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                recordsPerBatchMetricName,
                k -> new Histogram.Builder()
                        .name(recordsPerBatchMetricName)
                        .labelNames(labelNames)
                        .help("Records per batch histogram")
                        .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                        .register(registry));
        recordsPerBatchHistogram = recordsPerBatchMetric.labels(labelValues);

        String batchSizeBytesMetricName = String.format("%s_batched_log_entry_size_bytes", metricsPrefix);
        batchSizeBytesMetric = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                batchSizeBytesMetricName,
                k -> new Histogram.Builder()
                        .name(batchSizeBytesMetricName)
                        .labelNames(labelNames)
                        .help("Batch size in bytes histogram")
                        .buckets(BYTES_SIZE_PER_ENTRY_BUCKETS)
                        .register(registry));
        batchSizeBytesHistogram = batchSizeBytesMetric.labels(labelValues);

        String oldestRecordInBatchDelayTimeSecondsMetricName =
                String.format("%s_batched_log_oldest_record_delay_time_seconds", metricsPrefix);
        oldestRecordInBatchDelayTimeSecondsMetric = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                oldestRecordInBatchDelayTimeSecondsMetricName,
                k -> new Histogram.Builder()
                        .name(oldestRecordInBatchDelayTimeSecondsMetricName)
                        .labelNames(labelNames)
                        .help("Max record latency in batch histogram")
                        .buckets(MAX_DELAY_TIME_BUCKETS)
                        .register(registry));
        oldestRecordInBatchDelayTimeSecondsHistogram =
                oldestRecordInBatchDelayTimeSecondsMetric.labels(labelValues);

        String batchFlushTriggeringByMaxRecordsMetricName =
                String.format("%s_batched_log_triggering_count_by_records", metricsPrefix);
        batchFlushTriggeringByMaxRecordsMetric = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                batchFlushTriggeringByMaxRecordsMetricName,
                k -> new Counter.Builder()
                        .name(batchFlushTriggeringByMaxRecordsMetricName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold"
                                + " \"batchedWriteMaxRecords\"")
                        .register(registry));
        batchFlushTriggeringByMaxRecordsCounter = batchFlushTriggeringByMaxRecordsMetric.labels(labelValues);

        String batchFlushTriggeringByMaxSizeMetricName =
                String.format("%s_batched_log_triggering_count_by_size", metricsPrefix);
        batchFlushTriggeringByMaxSizeMetric = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                batchFlushTriggeringByMaxSizeMetricName,
                k -> new Counter.Builder()
                        .name(batchFlushTriggeringByMaxSizeMetricName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold \"batchedWriteMaxSize\"")
                        .register(registry));
        batchFlushTriggeringByMaxSizeCounter = batchFlushTriggeringByMaxSizeMetric.labels(labelValues);

        String batchFlushTriggeringByMaxDelayMetricName =
                String.format("%s_batched_log_triggering_count_by_delay_time", metricsPrefix);
        batchFlushTriggeringByMaxDelayMetric = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                batchFlushTriggeringByMaxDelayMetricName,
                k -> new Counter.Builder()
                        .name(batchFlushTriggeringByMaxDelayMetricName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold"
                                + " \"batchedWriteMaxDelayInMillis\"")
                        .register(registry));
        batchFlushTriggeringByMaxDelayCounter =
                batchFlushTriggeringByMaxDelayMetric.labels(labelValues);

        String batchFlushTriggeringByLargeSingleDataMetricName =
                String.format("%s_batched_log_triggering_count_by_force", metricsPrefix);
        batchFlushTriggeringByLargeSingleDataMetric = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                batchFlushTriggeringByLargeSingleDataMetricName,
                k -> new Counter.Builder()
                        .name(batchFlushTriggeringByLargeSingleDataMetricName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold \"forceFlush\"")
                        .register(registry));
        batchFlushTriggeringByLargeSingleDataCounter = batchFlushTriggeringByLargeSingleDataMetric.labels(labelValues);
    }

    public void triggerFlushByRecordsCount(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeringByMaxRecordsCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByBytesSize(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeringByMaxSizeCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByByMaxDelay(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeringByMaxDelayCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByForce(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeringByLargeSingleDataCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    /**
     * Append the metrics which is type of histogram.
     */
    private void observeHistogram(int recordCount, long bytesSize, long delayMillis) {
        recordsPerBatchHistogram.observe(recordCount);
        batchSizeBytesHistogram.observe(bytesSize);
        oldestRecordInBatchDelayTimeSecondsHistogram.observe(delayMillis);
    }
}
