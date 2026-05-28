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
package org.apache.pulsar.transaction.coordinator.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.EndedTxnStatus;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreConfig;
import org.jspecify.annotations.Nullable;

class EndedTxnStatusCache {

    private final Cache<TxnID, EndedTxnMetadata> cache;
    private final long retentionTimeMs;
    private final AtomicBoolean closed;

    private EndedTxnStatusCache(Cache<TxnID, EndedTxnMetadata> cache, long retentionTimeMs, AtomicBoolean closed) {
        this.cache = cache;
        this.retentionTimeMs = retentionTimeMs;
        this.closed = closed;
    }

    static EndedTxnStatusCache create(long retentionTimeMs, long maxRecordCount,
                                      @Nullable Consumer<EndedTxnMetadata> removalListener) {
        TransactionMetadataStoreConfig.validateEndedStatusConfig(retentionTimeMs, maxRecordCount);
        AtomicBoolean closed = new AtomicBoolean(false);
        if (retentionTimeMs == 0 || maxRecordCount == 0) {
            return new EndedTxnStatusCache(null, retentionTimeMs, closed);
        }
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().executor(Runnable::run);
        if (retentionTimeMs > 0) {
            cacheBuilder.expireAfter(new Expiry<>() {
                @Override
                public long expireAfterCreate(Object key, Object rawValue, long currentTime) {
                    EndedTxnMetadata value = (EndedTxnMetadata) rawValue;
                    long expireAtMs = value.endedAtMs() + retentionTimeMs;
                    long remainingMs = expireAtMs - System.currentTimeMillis();
                    return remainingMs <= 0 ? 0 : TimeUnit.MILLISECONDS.toNanos(remainingMs);
                }

                @Override
                public long expireAfterUpdate(Object key, Object value, long currentTime,
                                              long currentDuration) {
                    return expireAfterCreate(key, value, currentTime);
                }

                @Override
                public long expireAfterRead(Object key, Object value, long currentTime,
                                            long currentDuration) {
                    return currentDuration;
                }
            });
        }
        if (maxRecordCount > 0) {
            cacheBuilder.maximumSize(maxRecordCount);
        }
        if (removalListener != null) {
            cacheBuilder.removalListener((txnID, value, cause) -> {
                if (!closed.get() && value != null) {
                    removalListener.accept((EndedTxnMetadata) value);
                }
            });
        }
        Cache<TxnID, EndedTxnMetadata> cache = cacheBuilder.build();
        return new EndedTxnStatusCache(cache, retentionTimeMs, closed);
    }

    EndedTxnStatus get(TxnID txnID) {
        if (cache == null) {
            return null;
        }
        EndedTxnMetadata metadata = cache.getIfPresent(txnID);
        return metadata == null ? null : metadata.status();
    }

    boolean record(TxnID txnID, EndedTxnStatus status, Position logPositions, long endedAtMs) {
        if (cache == null || status == null || !isWithinRetention(endedAtMs)) {
            return false;
        }
        cache.put(txnID, new EndedTxnMetadata(txnID, status, logPositions, endedAtMs));
        return true;
    }

    void close() {
        if (cache != null) {
            closed.set(true);
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    private boolean isWithinRetention(long endedAtMs) {
        return retentionTimeMs < 0 || endedAtMs + retentionTimeMs > System.currentTimeMillis();
    }

    record EndedTxnMetadata(TxnID txnID, EndedTxnStatus status, Position logPosition, long endedAtMs) {}
}
