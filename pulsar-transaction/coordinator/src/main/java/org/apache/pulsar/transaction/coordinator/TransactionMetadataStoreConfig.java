/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.coordinator;

import java.util.concurrent.TimeUnit;

/**
 * Configuration owned by a transaction metadata store instance.
 */
public class TransactionMetadataStoreConfig {

    public static final long DEFAULT_TRANSACTION_ENDED_STATUS_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(1);
    public static final long DEFAULT_TRANSACTION_ENDED_STATUS_MAX_RECORD_COUNT = 100_000L;

    private final long maxActiveTransactionsPerCoordinator;
    private final long transactionEndedStatusRetentionTimeMs;
    private final long transactionEndedStatusMaxRecordCount;

    public TransactionMetadataStoreConfig(long maxActiveTransactionsPerCoordinator,
                                          long transactionEndedStatusRetentionTimeMs,
                                          long transactionEndedStatusMaxRecordCount) {
        validateEndedStatusConfig(transactionEndedStatusRetentionTimeMs, transactionEndedStatusMaxRecordCount);
        this.maxActiveTransactionsPerCoordinator = maxActiveTransactionsPerCoordinator;
        this.transactionEndedStatusRetentionTimeMs = transactionEndedStatusRetentionTimeMs;
        this.transactionEndedStatusMaxRecordCount = transactionEndedStatusMaxRecordCount;
    }

    public static TransactionMetadataStoreConfig defaultConfig(long maxActiveTransactionsPerCoordinator) {
        return new TransactionMetadataStoreConfig(maxActiveTransactionsPerCoordinator,
                DEFAULT_TRANSACTION_ENDED_STATUS_RETENTION_TIME_MS,
                DEFAULT_TRANSACTION_ENDED_STATUS_MAX_RECORD_COUNT);
    }

    public static void validateEndedStatusConfig(long retentionTimeMs, long maxRecordCount) {
        if (retentionTimeMs < 0 && maxRecordCount < 0) {
            throw new IllegalArgumentException("Configuration fields 'transactionEndedStatusRetentionTimeMs' and "
                    + "'transactionEndedStatusMaxRecordCount' cannot both be negative because that would retain "
                    + "ended transaction statuses without a time or size limit, eventually, it will cause OOM error");
        }
    }

    public long getMaxActiveTransactionsPerCoordinator() {
        return maxActiveTransactionsPerCoordinator;
    }

    public long getTransactionEndedStatusRetentionTimeMs() {
        return transactionEndedStatusRetentionTimeMs;
    }

    public long getTransactionEndedStatusMaxRecordCount() {
        return transactionEndedStatusMaxRecordCount;
    }
}
