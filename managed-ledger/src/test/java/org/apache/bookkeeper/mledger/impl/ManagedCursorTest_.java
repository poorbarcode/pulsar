package org.apache.bookkeeper.mledger.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.eclipse.jetty.util.BlockingArrayQueue;

import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ManagedCursorTest_  extends MockedBookKeeperTestCase {

    @Test
    public void testCloseCursor() throws Exception {
        // Create ManagedLedger and ex.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        // Start a reader.
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) managedLedger.openCursor("cursor1");
        ReadEntryTask readEntryTask = new ReadEntryTask(cursor1);
        readEntryTask.start();

        final Callback callback = new Callback();
        // Write some data.
        for (int i = 0; i < 20; i++){
            managedLedger.asyncAddEntry(new byte[]{1}, callback, i);
        }
        Awaitility.await().until(() -> callback.positions.size() == 20);

        // Wait read any entry.
        Awaitility.await().atMost(Duration.ofMinutes(2)).until(() -> readEntryTask.getReceivedEntries().size() >= 1);

        // Verify.
        log.info("positions: {}", callback.positions);
        managedLedger.close();
        for (Entry entry : readEntryTask.getReceivedEntries()){
            Assert.assertTrue(managedLedger.getLedgersInfo().containsKey(entry.getLedgerId()));
        }
    }

    @Test(timeOut = 120 * 1000, invocationCount = 200)
    public void testCloseCursor2() throws Exception {
        // Create ManagedLedger and mock timeout ex.
        String managedLedgerName = "ml_" + UUID.randomUUID().toString().replaceAll("-", "_");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setAddEntryTimeoutSeconds(1);
//        config.setReadEntryTimeoutSeconds(1);
        PulsarMockBookKeeper_ bkc = new PulsarMockBookKeeper_(executor);
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        Field f = ManagedLedgerFactoryConfig.class.getDeclaredField("numManagedLedgerSchedulerThreads");
        f.setAccessible(true);
        f.set(factoryConfig, 100);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open(managedLedgerName, config);

        // Start a reader.
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) managedLedger.openCursor("cursor1");
        ReadEntryTask readEntryTask = new ReadEntryTask(cursor1);
        readEntryTask.start();

        // Write some data.
        final Callback callback = new Callback();
        for (int i = 0; i < 20; i++){
            managedLedger.asyncAddEntry(new byte[]{1}, callback, i);
        }
        Awaitility.await().atMost(Duration.ofMinutes(2))
                .until(() -> callback.positions.size() >= 10);

        log.info("positions: {}", callback.positions);

        // Wait read any entry.
        Awaitility.await().atMost(Duration.ofMinutes(2))
                .until(() -> readEntryTask.getReceivedEntries().size() >= 1);

        // Verify
        managedLedger.close();
        for (Entry entry : readEntryTask.getReceivedEntries()){
            Assert.assertTrue(managedLedger.getLedgersInfo().containsKey(entry.getLedgerId()));
        }
    }

    private static class Callback implements AsyncCallbacks.AddEntryCallback{

        public static final ConcurrentSkipListMap<Integer, Object> positions = new ConcurrentSkipListMap<>();

        @Override
        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
            positions.put(Integer.valueOf(ctx.toString()), position);
        }

        @Override
        public void addFailed(ManagedLedgerException exception, Object ctx) {
            positions.put(Integer.valueOf(ctx.toString()), "ex");
        }
    }

    private class PulsarMockBookKeeper_ extends PulsarMockBookKeeper{

        public PulsarMockBookKeeper_(OrderedExecutor orderedExecutor) throws Exception {
            super(orderedExecutor);
        }

        @Override
        public void asyncCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, final DigestType digestType,
                                      final byte[] passwd, final AsyncCallback.CreateCallback cb, final Object ctx,
                                      Map<String, byte[]> properties) {
            AsyncCallback.CreateCallback mockCb = (rc, lh, ctx1) -> {
                log.info("===> create ledger: {}", lh.getId());
                cb.createComplete(rc, spyLedgerHandle(lh), ctx1);
            };
            super.asyncCreateLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, mockCb, ctx,
                    properties);
        }
    }

    private LedgerHandle spyLedgerHandle(final LedgerHandle originalLedgerHandle){
        LedgerHandle spyLedgerHandle = spy(originalLedgerHandle);
        final long ledgerId = originalLedgerHandle.getId();
        doAnswer(invocation -> {
            ByteBuf byteBuf = (ByteBuf) invocation.getArguments()[0];
            AsyncCallback.AddCallback cb = (AsyncCallback.AddCallback) invocation.getArguments()[1];
            Object ctx = invocation.getArguments()[2];
            AtomicInteger counter = new AtomicInteger();
            AsyncCallback.AddCallback mockCb = (rc, lh, entryId, ctx1) -> {
                if (counter.incrementAndGet() % 3 == 0){
                    try {
                        Thread.sleep(2 * 1000);
                    } catch (InterruptedException e) {
                    }
                    log.info("===> Async add entry callback timeout {}:{}, lh.lastConfirmed: {}, lh: {}",
                            ledgerId, entryId, lh.getLastAddConfirmed(), lh);
                } else {
                    log.info("===> Async add entry callback {}:{}, lh.lastConfirmed: {}, lh: {}",
                            ledgerId, entryId, lh.getLastAddConfirmed(), lh);
                }
                cb.addComplete(rc, lh, entryId, ctx1);
            };
            originalLedgerHandle.asyncAddEntry(byteBuf, mockCb, ctx);
            return null;
        }).when(spyLedgerHandle).asyncAddEntry(any(ByteBuf.class), any(AsyncCallback.AddCallback.class), any());
        doAnswer(invocation -> originalLedgerHandle.getLastAddConfirmed()).when(spyLedgerHandle).getLastAddConfirmed();
        log.info("create ledgerHandle spy: {}, original: {}", spyLedgerHandle, originalLedgerHandle);
        return spyLedgerHandle;
    }

    private static class ReadEntryTask implements Runnable {

        private final ManagedCursorImpl cursor;
        @Getter
        private final List<Entry> receivedEntries = new BlockingArrayQueue<>();
        @Getter
        private final List<Exception> receivedEntryErrors = new BlockingArrayQueue<>();

        public ReadEntryTask(ManagedCursorImpl cursor){
            this.cursor = cursor;
        }

        @Override
        public void run() {

            final Object ctx = new Object();
            cursor.asyncReadEntriesOrWait(100, new AsyncCallbacks.ReadEntriesCallback(){

                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    receivedEntries.addAll(entries);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    receivedEntryErrors.add(exception);
                }
            }, ctx, PositionImpl.LATEST);
        }

        public void start(){
            new Thread(this).start();
        }
    }
}
