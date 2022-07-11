package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.proto.BatchedTransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MLTransactionLogBatchTest {

    @Test
    public void testReadingEntriesInRecover() throws Exception{
        // Process Controller
        final AtomicInteger processController = new AtomicInteger();
        // Mock resources.
        final ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        final ManagedCursor managedCursor = Mockito.mock(ManagedCursorImpl.class);
        final TransactionLogReplayCallback transactionLogReplayCallback =
                Mockito.mock(TransactionLogReplayCallback.class);
        final ManagedLedgerConfig managedLedgerConfig = Mockito.mock(ManagedLedgerConfig.class);
        ManagedLedgerFactory managedLedgerFactory = Mockito.mock(ManagedLedgerFactory.class);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                processController.incrementAndGet();
                return null;
            }
        }).when(transactionLogReplayCallback).replayComplete();
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.OpenLedgerCallback callback =
                        (AsyncCallbacks.OpenLedgerCallback) invocation.getArguments()[2];
                callback.openLedgerComplete(managedLedger, invocation.getArguments()[4]);
                return null;
            }
        }).when(managedLedgerFactory).asyncOpen(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.OpenCursorCallback callback =
                        (AsyncCallbacks.OpenCursorCallback) invocation.getArguments()[2];
                callback.openCursorComplete(managedCursor, invocation.getArguments()[3]);
                return null;
            }
        }).when(managedLedger).asyncOpenCursor(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        // Mock non batched Entries.
        final List<Entry> entryList = new ArrayList<>();
        final List<TransactionMetadataEntry> logList = new ArrayList<>();
        for (int i = 0; i < 2; i++){
            TransactionMetadataEntry pendingAckMetadataEntry = new TransactionMetadataEntry();
            pendingAckMetadataEntry.setTxnidLeastBits(i);
            pendingAckMetadataEntry.setTxnidMostBits(i);
            pendingAckMetadataEntry.setMaxLocalTxnId(i);
            pendingAckMetadataEntry.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.ADD_PARTITION);
            pendingAckMetadataEntry.addAllPartitions(Collections.singletonList("" + i));
            pendingAckMetadataEntry.setLastModificationTime(System.currentTimeMillis());
            logList.add(pendingAckMetadataEntry);
            entryList.add(EntryImpl.create(i, i, pendingAckMetadataEntry.toByteArray()));
        }
        AtomicBoolean hasMoreEntries = new AtomicBoolean(true);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return hasMoreEntries.get();
            }
        }).when(managedCursor).hasMoreEntries();
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.ReadEntriesCallback callback =
                        (AsyncCallbacks.ReadEntriesCallback) invocation.getArguments()[1];
                if (hasMoreEntries.compareAndSet(true, false)) {
                    callback.readEntriesComplete(entryList, invocation.getArguments()[2]);
                } else {
                  callback.readEntriesComplete(Collections.emptyList(), invocation.getArguments()[2]);
                }
                return null;
            }
        }).when(managedCursor).asyncReadEntries(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());

        // Assert replay non-batched transaction log correct.
        MLTransactionLogImpl transactionLog = new MLTransactionLogImpl(TransactionCoordinatorID.get(1),
                managedLedgerFactory, managedLedgerConfig);
        transactionLog.initialize().get();
        LinkedHashMap<Long, TransactionMetadataEntry> handleMap = new LinkedHashMap<>();
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Position position = (Position) invocation.getArguments()[0];
                if (position instanceof PositionImpl && ((PositionImpl) position).getAckSet() != null){
                    handleMap.put(position.getEntryId() * 100 + ((PositionImpl) position).getAckSet()[0],
                            (TransactionMetadataEntry) invocation.getArguments()[1]);
                } else {
                    handleMap.put(position.getEntryId(), (TransactionMetadataEntry) invocation.getArguments()[1]);
                }
                return null;
            }
        }).when(transactionLogReplayCallback).handleMetadataEntry(Mockito.any(), Mockito.any());
        transactionLog.replayAsync(transactionLogReplayCallback);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 1);
        Assert.assertEquals(handleMap.size(), 2);
        Assert.assertEquals(handleMap.get(0L).getMaxLocalTxnId(), 0L);
        Assert.assertEquals(handleMap.get(1L).getMaxLocalTxnId(), 1L);
        // Clear buffers.
        handleMap.clear();
        hasMoreEntries.set(true);

        // Assert replay batched transaction log correct.
        BatchedTransactionMetadataEntry batchedTransactionMetadataEntry = new BatchedTransactionMetadataEntry();
        batchedTransactionMetadataEntry.addAllTransactionLogs(logList);
        ByteBuf byteBuf = Unpooled.buffer(4);
        byteBuf.writeShort(TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER);
        byteBuf.writeShort(TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_VERSION);
        final Entry entry = EntryImpl.create(2L, 3L, Unpooled.wrappedUnmodifiableBuffer(byteBuf,
                Unpooled.copiedBuffer(batchedTransactionMetadataEntry.toByteArray())));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.ReadEntriesCallback callback =
                        (AsyncCallbacks.ReadEntriesCallback) invocation.getArguments()[1];
                if (hasMoreEntries.compareAndSet(true, false)) {
                    callback.readEntriesComplete(Collections.singletonList(entry), invocation.getArguments()[2]);
                } else {
                    callback.readEntriesComplete(Collections.emptyList(), invocation.getArguments()[2]);
                }
                return null;
            }
        }).when(managedCursor).asyncReadEntries(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());
        transactionLog.replayAsync(transactionLogReplayCallback);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 2);
        Assert.assertEquals(handleMap.size(), 2);
        Assert.assertEquals(handleMap.get(301L).getMaxLocalTxnId(), 0L);
        Assert.assertEquals(handleMap.get(302L).getMaxLocalTxnId(), 1L);

        // Nothing should be cleanup.
    }
}
