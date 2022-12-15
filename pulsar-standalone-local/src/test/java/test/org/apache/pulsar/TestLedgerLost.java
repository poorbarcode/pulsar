package test.org.apache.pulsar;

import static org.apache.pulsar.LockCoordinator.PROCESS_COODINATOR;
import static org.apache.pulsar.LockCoordinator.waitForValue;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.PulsarStandaloneStarter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.awaitility.Awaitility;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class TestLedgerLost {

    private PulsarService pulsar;

    private ManagedLedgerFactoryImpl managedLedgerFactory;
    @BeforeClass
    public void testLedgerLost() throws Exception {
        PROCESS_COODINATOR.set(100);
        String[] args = new String[]{
                "--no-functions-worker",
                "--config",
                "/Users/fengyubiao/data/git/fork/pulsar/conf/standalone.conf"
        };
        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        standalone.start();
        pulsar = standalone.getBroker();
        managedLedgerFactory = (ManagedLedgerFactoryImpl) pulsar.getBrokerService().getManagedLedgerFactory();
        Thread.sleep(3 * 1000);
    }

    private void triggerLedgerRollover(ManagedLedger managedLedger, int maxEntriesPerLedger) {
        new Thread(() -> {
            int writeLedgerCount = 2;
            for (int i = 0; i < writeLedgerCount; i++) {
                for (int j = 0; j < maxEntriesPerLedger; j++) {
                    byte[] data = String.format("%s_%s", i, j).getBytes(Charset.defaultCharset());
                    Object ctx = "";
                    managedLedger.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                        @Override
                        public void addComplete(Position position, ByteBuf entryData, Object ctx) {

                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {

                        }
                    }, ctx);
                }
            }
        }).start();
    }

    private int calculateCursorCount(ManagedLedgerImpl ledger){
        Iterator iterator = ledger.getCursors().iterator();
        int count = 0;
        while (iterator.hasNext()){
            iterator.next();
            count++;
        }
        return count;
    }

    private ManagedLedgerConfig createManagedLedgerConfig(int maxEntriesPerLedger){
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(maxEntriesPerLedger);
        config.setEnsembleSize(1);
        config.setWriteQuorumSize(1);
        config.setAckQuorumSize(1);
        return config;
    }

    private void printLedgers(String managedLedgerName, ManagedLedgerImpl managedLedger){
        log.info("===> [{}] currentLedger: {}, currentLedger-lastConfirm: {}, \n ledgers: {}",
                managedLedgerName,
                managedLedger.getCurrentLedger().getId(),
                managedLedger.getCurrentLedger().getLastAddConfirmed(),
                managedLedger.getLedgersInfo().toString().replaceAll("\n", "    \n"));
    }

    private void writeSomeEntries(ManagedLedgerImpl managedLedger, int entryCount,
                                  BlockingArrayQueue<Position> positionCollector){
        for (int i = 0; i < entryCount; i++) {
            byte[] data = String.valueOf(i).getBytes(Charset.defaultCharset());
            Object ctx = "";
            managedLedger.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    positionCollector.add(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Write entry fail", exception);
                }
            }, ctx);
        }
    }

    private List<Position> readEntries(final ManagedCursorImpl managedCursor, int readCount) throws Exception {
        final List<Position> positionList = new BlockingArrayQueue<>();
        new Thread(() -> {
            while (positionList.size() < readCount) {
                managedCursor.asyncReadEntriesOrWait(readCount, new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        positionList.addAll(
                                entries.stream()
                                        .map(entry -> PositionImpl.get(entry.getLedgerId(), entry.getEntryId())).
                                        collect(Collectors.toList())
                        );
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

                    }
                }, "", PositionImpl.LATEST);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        }).start();
        Awaitility.await().until(() -> positionList.size() >= readCount);
        managedCursor.delete(positionList.get(positionList.size() - 1));
        return positionList;
    }

    @Test
    public void testConcurrentCloseLedgerAndSwitchLedger() throws Exception {
        String managedLedgerName = "lg_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String cursorName1 = "cs_01";
        String cursorName2 = "cs_02";
        int maxEntriesPerLedger = 5;

        // call "switch ledger" and "managedLedger.close" concurrently.
        ManagedLedgerConfig config = createManagedLedgerConfig(maxEntriesPerLedger);
        final ManagedLedgerImpl managedLedger1 =
                (ManagedLedgerImpl) managedLedgerFactory.open(managedLedgerName, config);
        PROCESS_COODINATOR.set(0);
        triggerLedgerRollover(managedLedger1, maxEntriesPerLedger);
        managedLedger1.close();

        // 1. close managedLedger1.
        // 2. create new ManagedLedger: managedLedger2.
        //   2-1: create cursor1 of managedLedger2.
        // 3. close managedLedger1 twice, make managedLedger2 remove from cache of managedLedgerFactory.
        // 4. create new ManagedLedger: managedLedger3.
        //   4-2: create cursor2 of managedLedger3.
        // Then two ManagedLedger appear at the same time and have different numbers of cursors.
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(managedLedger1.getState() != ManagedLedgerImpl.State.Closed));
        Thread.sleep(1000);
        final ManagedLedgerImpl managedLedger2 =(ManagedLedgerImpl) managedLedgerFactory
                .open(managedLedgerName, createManagedLedgerConfig(100));
        Awaitility.await().until(() -> {
            CompletableFuture<ManagedLedgerImpl> managedLedgerFuture =
                    managedLedgerFactory.ledgers.get(managedLedgerName);
            return managedLedgerFuture.join() == managedLedger2;
        });
        managedLedger1.close();
        printLedgers("step1 managedLedger1", managedLedger1);
        ManagedCursorImpl managedCursor2 =
                (ManagedCursorImpl) managedLedger2.openCursor(cursorName1, CommandSubscribe.InitialPosition.Earliest);
        Assert.assertEquals(managedLedger1.getState(), ManagedLedgerImpl.State.Closed);
        Assert.assertTrue(managedLedgerFactory.ledgers.isEmpty());

        CompletableFuture<ManagedLedgerImpl> createManagedLedger3Future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                ManagedLedgerImpl managedLedger3 = (ManagedLedgerImpl) managedLedgerFactory
                        .open(managedLedgerName, createManagedLedgerConfig(200));
                managedLedger3.openCursor(cursorName2, CommandSubscribe.InitialPosition.Earliest);
                Assert.assertTrue(managedLedger2.getState() != ManagedLedgerImpl.State.Closed);
                Assert.assertTrue(managedLedger3.getState() != ManagedLedgerImpl.State.Closed);
                Assert.assertEquals(calculateCursorCount(managedLedger2), 1);
                Assert.assertEquals(calculateCursorCount(managedLedger3), 2);
                createManagedLedger3Future.complete(managedLedger3);
            } catch (Exception ex){
                createManagedLedger3Future.completeExceptionally(ex);
            }
        }).start();

        printLedgers("step2 managedLedger2", managedLedger2);
        BlockingArrayQueue<Position> positionCollector = new BlockingArrayQueue<>();
        writeSomeEntries(managedLedger2, 10, positionCollector);
        waitForValue(3, 4);
//        log.info("===> wait 4--> 5, choose thread: {}",
//                managedLedger2.getExecutor().chooseThread(managedLedger2.getName()));
//        log.info("===> wait 5--> 6, choose thread: {}",
//                managedLedger2.getExecutor().chooseThread(managedLedger2.getCurrentLedger().getId()));
        Awaitility.await().until(() -> {
            return positionCollector.size() == 10;
        });
        readEntries(managedCursor2, 10);
        waitForValue(7, 8);

        ManagedLedgerImpl managedLedger3 = createManagedLedger3Future.join();

        printLedgers("step3 managedLedger2", managedLedger2);
        printLedgers("step3 managedLedger3", managedLedger3);

        System.out.println(1);
    }
}
