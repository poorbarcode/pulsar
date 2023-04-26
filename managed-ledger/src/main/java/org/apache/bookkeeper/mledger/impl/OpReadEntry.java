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
package org.apache.bookkeeper.mledger.impl;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.LinearReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpReadEntry implements ReadEntriesCallback {

    ManagedCursorImpl cursor;
    PositionImpl readPosition;
    private int count;
    private LinearReadEntriesCallback callback;
    Object ctx;

    // Results
    private List<Entry> entries;
    private PositionImpl nextReadPosition;

    private long cursorEpoch = Integer.MIN_VALUE;
    PositionImpl maxPosition;

    Predicate<PositionImpl> skipCondition;

    public static OpReadEntry create(ManagedCursorImpl cursor, PositionImpl readPositionRef, int count,
                                     LinearReadEntriesCallback callback, Object ctx, PositionImpl maxPosition,
                                     Predicate<PositionImpl> skipCondition) {
        OpReadEntry op = RECYCLER.get();
        op.readPosition = cursor.ledger.startReadOperationOnLedger(readPositionRef);
        op.cursor = cursor;
        op.cursorEpoch = cursor.getEpoch();
        op.count = count;
        op.callback = callback;
        op.entries = new ArrayList<>();
        if (maxPosition == null) {
            maxPosition = PositionImpl.LATEST;
        }
        op.maxPosition = maxPosition;
        op.skipCondition = skipCondition;
        op.ctx = ctx;
        op.nextReadPosition = PositionImpl.get(op.readPosition);
        return op;
    }

    boolean checkCursorNoChanges() {
        return cursor.getEpoch() == cursorEpoch;
    }

    void internalReadEntriesComplete(List<Entry> returnedEntries, Object ctx, PositionImpl lastPosition) {
        // Filter the returned entries for individual deleted messages
        int entriesCount = returnedEntries.size();
        long entriesSize = 0;
        for (int i = 0; i < entriesCount; i++) {
            entriesSize += returnedEntries.get(i).getLength();
        }
        cursor.updateReadStats(entriesCount, entriesSize);

        if (entriesCount != 0) {
            lastPosition = (PositionImpl) returnedEntries.get(entriesCount - 1).getPosition();
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Read entries succeeded batch_size={} cumulative_size={} requested_count={}",
                    cursor.ledger.getName(), cursor.getName(), returnedEntries.size(), entries.size(), count);
        }

        List<Entry> filteredEntries = Collections.emptyList();
        if (entriesCount != 0) {
            filteredEntries = cursor.filterReadEntries(returnedEntries);
            entries.addAll(filteredEntries);
        }

        // if entries have been filtered out then try to skip reading of already deletedMessages in that range
        final Position nexReadPosition = entriesCount != filteredEntries.size()
                ? cursor.getNextAvailablePosition(lastPosition) : lastPosition.getNext();
        updateReadPositionIfCursorEpochMatches(nexReadPosition);
        checkReadCompletion();
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        internalReadEntriesComplete(returnedEntries, ctx, null);
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        cursor.readOperationCompleted();

        if (!entries.isEmpty()) {
            // There were already some entries that were read before, we can return them
            cursor.ledger.getExecutor().execute(() -> {
                callback.readEntriesComplete(entries, checkCursorNoChanges(), ctx);
                recycle();
            });
        } else if (cursor.config.isAutoSkipNonRecoverableData() && exception instanceof NonRecoverableLedgerException) {
            log.warn("[{}][{}] read failed from ledger at position:{} : {}", cursor.ledger.getName(), cursor.getName(),
                    readPosition, exception.getMessage());
            final ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
            Position nexReadPosition;
            Long lostLedger = null;
            if (exception instanceof ManagedLedgerException.LedgerNotExistException) {
                // try to find and move to next valid ledger
                nexReadPosition = cursor.getNextLedgerPosition(readPosition.getLedgerId());
                lostLedger = readPosition.ledgerId;
            } else {
                // Skip this read operation
                nexReadPosition = ledger.getValidPositionAfterSkippedEntries(readPosition, count);
            }
            // fail callback if it couldn't find next valid ledger
            if (nexReadPosition == null) {
                callback.readEntriesFailed(exception, ctx);
                cursor.ledger.mbean.recordReadEntriesError();
                recycle();
                return;
            }
            updateReadPositionIfCursorEpochMatches(nexReadPosition);
            if (lostLedger != null) {
                cursor.getManagedLedger().skipNonRecoverableLedger(lostLedger);
            }
            checkReadCompletion();
        } else {
            if (!(exception instanceof TooManyRequestsException)) {
                log.warn("[{}][{}] read failed from ledger at position:{}", cursor.ledger.getName(),
                        cursor.getName(), readPosition, exception);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] read throttled failed from ledger at position:{}", cursor.ledger.getName(),
                            cursor.getName(), readPosition);
                }
            }
            // TODO 这里会不会丢数据？
            callback.readEntriesFailed(exception, ctx);
            cursor.ledger.mbean.recordReadEntriesError();
            recycle();
        }
    }

    /***
     * Solution: compare and swap.
     * case-1 start: 10; update_by_read: 10 -> 20; rewind: 10 -> 1. resulting repeated read.
     * case-2 start: 10; rewind: 1; update_by_read: 10 -> 20(update failure, read success).
     *          resulting repeated read and out of order(0~02, 0~20, it is OK).
     * case-3 start: 10; reset_cursor: 10 -> 30; update_by_read: 10 -> 20(update failure, read success).
     * case-4 start: 10; reset_cursor: 10 -> 20; update_by_read: 10 -> 20(update failure, read success).
     *          update_by_read: 20 -> 30(update success, read success).
     * case-5 start: 10; update_by_read: 10 -> 20; rewind: 10 -> 1, resulting repeated read;
     *          reading 20 -> 30
     *          reading 1 -> 10  will above read will complete first?
     *          TODO 如果直接用 cursor 的 API，多个 ledger 并不能保证顺序，
     *          TODO 因为缓存的存在同一个 ledger 也不能保证顺序。
     * @param newReadPosition
     */
    void updateReadPositionIfCursorEpochMatches(Position newReadPosition) {
        if (checkCursorNoChanges()) {
            nextReadPosition = (PositionImpl) newReadPosition;
            /**
             * Solution: water marker.
             * case-1 set_pos: true; readComplete: true; seek. it is OK.
             * case-2 seek; set_pos: false; readComplete: false. it is OK.
             * case-3 set_pos: true; seek; readComplete: false. it is OK, callback will discard these entries;
             * case-4 set_pos: true; readComplete: false; seek; do callback: false.
             *   TODO 怎么办？客户端需要保证 do-callback 和 seek 没有并发的可能。
             *   TODO 而且 checkCursorNoChanges() 需要 do-callback 自己完成。
             *   it is ok. 因为 cursor_change 之前的读取是连续的，cursor 之后的读取也是连续的，
             * case-5 checkCursorNoChanges:true; seek; set position.
             *   TODO 这个怎么办？加锁还是CAS？建议 read_lock, CAS 更好。
             */
            cursor.setReadPosition(nextReadPosition);
        }
    }

    void checkReadCompletion() {
        // op readPosition is smaller or equals maxPosition then can read again
        if (entries.size() < count && cursor.hasMoreEntries()
                && maxPosition.compareTo(readPosition) > 0) {

            // We still have more entries to read from the next ledger, schedule a new async operation
            cursor.ledger.getExecutor().execute(() -> {
                readPosition = cursor.ledger.startReadOperationOnLedger(nextReadPosition);
                cursor.ledger.asyncReadEntries(OpReadEntry.this);
            });
        } else {
            // The reading was already completed, release resources and trigger callback
            try {
                cursor.readOperationCompleted();

            } finally {
                cursor.ledger.getExecutor().execute(() -> {
                    callback.readEntriesComplete(entries, checkCursorNoChanges(), ctx);
                    recycle();
                });
            }
        }
    }

    public int getNumberOfEntriesToRead() {
        return count - entries.size();
    }

    private final Handle<OpReadEntry> recyclerHandle;

    private OpReadEntry(Handle<OpReadEntry> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<OpReadEntry> RECYCLER = new Recycler<OpReadEntry>() {
        @Override
        protected OpReadEntry newObject(Recycler.Handle<OpReadEntry> recyclerHandle) {
            return new OpReadEntry(recyclerHandle);
        }
    };

    public void recycle() {
        count = 0;
        cursor = null;
        readPosition = null;
        callback = null;
        ctx = null;
        entries = null;
        nextReadPosition = null;
        maxPosition = null;
        recyclerHandle.recycle(this);
        skipCondition = null;
        cursorEpoch = Integer.MIN_VALUE;
    }

    private static final Logger log = LoggerFactory.getLogger(OpReadEntry.class);
}
