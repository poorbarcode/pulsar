package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.checkerframework.checker.units.qual.A;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;

public class MLTransactionLogBatchTest {

    public void testRecover(){
        // Process Controller
        final AtomicInteger processController = new AtomicInteger();
        // Mock resources.
        final ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        final ManagedCursor managedCursor = Mockito.mock(ManagedCursor.class);
        final TransactionLogReplayCallback pendingAckHandle = Mockito.mock(TransactionLogReplayCallback.class);
        final ManagedLedgerConfig managedLedgerConfig = Mockito.mock(ManagedLedgerConfig.class);
        ManagedLedgerFactory managedLedgerFactory = Mockito.mock(ManagedLedgerFactory.class);
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
        for (int i = 0; i < 100; i++){
            TransactionMetadataEntry pendingAckMetadataEntry = new TransactionMetadataEntry();
            PendingAckMetadata pendingAckMetadata = new PendingAckMetadata();
            pendingAckMetadata.setLedgerId(i);
            pendingAckMetadata.setEntryId(i);
            pendingAckMetadata.setBatchSize(1);
            pendingAckMetadataEntry.addAllPendingAckMetadatas(Collections.singletonList(pendingAckMetadata));
            pendingAckMetadataEntry.setAckType(CommandAck.AckType.Individual);
            pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ACK);
            pendingAckMetadataEntry.setTxnidLeastBits(i);
            pendingAckMetadataEntry.setTxnidMostBits(i);
            logList.add(pendingAckMetadataEntry);
            entryList.add(EntryImpl.create(i, i, pendingAckMetadataEntry.toByteArray()));
        }


        MLTransactionLogImpl transactionLog = new MLTransactionLogImpl(TransactionCoordinatorID.get(1),
                managedLedgerFactory, managedLedgerConfig);


        ManagedCursor subCursor = Mockito.mock(ManagedCursor.class);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Mockito.when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(executorService);
        Mockito.when(pendingAckHandle.changeToReadyState()).thenReturn(true);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                processController.incrementAndGet();
                return null;
            }
        }).when(pendingAckHandle).completeHandleFuture();
        // Mock data.
        Mockito.when(managedLedger.getLastConfirmedEntry()).thenReturn(PositionImpl.get(99,99));
        Mockito.when(managedCursor.getMarkDeletedPosition()).thenReturn(PositionImpl.EARLIEST);
        Mockito.when(managedCursor.hasMoreEntries()).thenReturn(true);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.ReadEntriesCallback callback =
                        (AsyncCallbacks.ReadEntriesCallback) invocation.getArguments()[1];
                callback.readEntriesComplete(entryList, invocation.getArguments()[2]);
                return null;
            }
        }).when(managedCursor).asyncReadEntries(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());
        // Test non batched entries replay.
        MLPendingAckStore mlPendingAckStore = new MLPendingAckStore(managedLedger, managedCursor, subCursor, 10);
        mlPendingAckStore.replayAsync(pendingAckHandle, executorService);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 1);
        Assert.assertEquals(mlPendingAckStore.pendingAckLogIndex.size(), 4);
        Iterator<Map.Entry<PositionImpl, PositionImpl>> iterator = mlPendingAckStore.pendingAckLogIndex.entrySet().iterator();
        Map.Entry<PositionImpl, PositionImpl> entry1 = iterator.next();
        Assert.assertEquals(entry1.getKey().getEntryId(), 9);
        Assert.assertEquals(entry1.getValue().getEntryId(), 9);
        Map.Entry<PositionImpl, PositionImpl> entry2 = iterator.next();
        Assert.assertEquals(entry2.getKey().getEntryId(), 19);
        Assert.assertEquals(entry2.getValue().getEntryId(), 19);
        Map.Entry<PositionImpl, PositionImpl> entry3 = iterator.next();
        Assert.assertEquals(entry3.getKey().getEntryId(), 39);
        Assert.assertEquals(entry3.getValue().getEntryId(), 39);
        Map.Entry<PositionImpl, PositionImpl> entry4 = iterator.next();
        Assert.assertEquals(entry4.getKey().getEntryId(), 69);
        Assert.assertEquals(entry4.getValue().getEntryId(), 69);
        log.info("Mock batched entries.");
        // Mock batched entries.
        Mockito.when(managedLedger.getLastConfirmedEntry()).thenReturn(PositionImpl.get(6,6));
        entryList.clear();
        for (int i = 0; i < 7; i++){
            BatchedPendingAckMetadataEntry batchedPendingAckMetadataEntry = new BatchedPendingAckMetadataEntry();
            batchedPendingAckMetadataEntry.addAllPendingAckLogs(logList.subList(i * 10, (i+1) * 10));
            ByteBuf byteBuf = Unpooled.buffer(4);
            byteBuf.writeShort(TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER);
            byteBuf.writeShort(TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_VERSION);
            entryList.add(EntryImpl.create(i, i, Unpooled.wrappedUnmodifiableBuffer(byteBuf,
                    Unpooled.copiedBuffer(batchedPendingAckMetadataEntry.toByteArray()))));
        }
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallbacks.ReadEntriesCallback callback =
                        (AsyncCallbacks.ReadEntriesCallback) invocation.getArguments()[1];
                callback.readEntriesComplete(entryList, invocation.getArguments()[2]);
                return null;
            }
        }).when(managedCursor).asyncReadEntries(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());
        // Test non batched entries replay.
        MLPendingAckStore mlPendingAckStoreBatched = new MLPendingAckStore(managedLedger, managedCursor, subCursor, 1);
        mlPendingAckStoreBatched.replayAsync(pendingAckHandle, executorService);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 2);
        Assert.assertEquals(mlPendingAckStoreBatched.pendingAckLogIndex.size(), 4);
        iterator = mlPendingAckStoreBatched.pendingAckLogIndex.entrySet().iterator();
        entry1 = iterator.next();
        Assert.assertEquals(entry1.getKey().getEntryId(), 9);
        Assert.assertEquals(entry1.getValue().getEntryId(), 0);
        entry2 = iterator.next();
        Assert.assertEquals(entry2.getKey().getEntryId(), 19);
        Assert.assertEquals(entry2.getValue().getEntryId(), 1);
        entry3 = iterator.next();
        Assert.assertEquals(entry3.getKey().getEntryId(), 39);
        Assert.assertEquals(entry3.getValue().getEntryId(), 3);
        entry4 = iterator.next();
        Assert.assertEquals(entry4.getKey().getEntryId(), 69);
        Assert.assertEquals(entry4.getValue().getEntryId(), 6);
    }
}
