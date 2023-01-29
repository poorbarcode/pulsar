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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class StatsBackLogTest extends ProducerConsumerBase {

    private static final int MAX_ENTRY_COUNT_PER_LEDGER = 10;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    protected void doInitConf() throws Exception {
        conf.setManagedLedgerMaxEntriesPerLedger(5);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private PersistentTopic getPersistentTopic(String topicName){
        return (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
    }

    private ManagedLedgerImpl getManagedLedger(String topicName){
        return (ManagedLedgerImpl) getPersistentTopic(topicName).getManagedLedger();
    }

    private ManagedCursorImpl getManagedCursor(String topicName, String subName){
        for (ManagedCursor cursor : getManagedLedger (topicName).getCursors()){
            if (cursor.getName().equals(subName)){
                return (ManagedCursorImpl) cursor;
            }
        }
        return null;
    }

    private void trimLedger(String topicName){
        trimLedgerAsync(topicName).join();
    }

    private CompletableFuture trimLedgerAsync(String topicName){
        CompletableFuture future = new CompletableFuture();
        getManagedLedger(topicName).internalTrimLedgers(false, future);
        return future;
    }

    private void awaitForMarkDeletedPosition(String topicName, String subName, long ledgerId, long entryId){
        ManagedCursorImpl cursor = getManagedCursor(topicName, subName);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(cursor.getMarkDeletedPosition().getLedgerId() >= ledgerId);
            assertTrue(cursor.getMarkDeletedPosition().getEntryId() >= entryId);
        });
    }

    private SendInfo sendMessages(int ledgerCount, int entryCountPerLedger, Producer<String> producer,
                                      String topicName) throws Exception{
        List<MessageId> messageIds = new ArrayList<>(ledgerCount * entryCountPerLedger);
        MessageIdImpl startMessageId = null;
        MessageIdImpl lastMessageId = null;
        for (int m = 0; m < ledgerCount; m++) {
            for (int n = 0; n < entryCountPerLedger; n++) {
                lastMessageId = (MessageIdImpl) producer.send(String.format("%s:%s", m, n));
                if (startMessageId == null){
                    startMessageId = lastMessageId;
                }
                messageIds.add(lastMessageId);
            }
            if (entryCountPerLedger < MAX_ENTRY_COUNT_PER_LEDGER) {
                admin.topics().unload(topicName);
            }
        }
        return new SendInfo(messageIds, startMessageId, lastMessageId);
    }

    private void consumeAllMessages(Consumer<String> consumer) throws Exception {
        while (true){
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null){
                break;
            }
            consumer.acknowledge(message);
        }
    }

    @AllArgsConstructor
    private static class SendInfo {
        List<MessageId> messageIds;
        MessageIdImpl firstMessageId;
        MessageIdImpl lastMessageId;
    }

    @DataProvider(name = "entryCountPerLedger")
    public Object[][] entryCountPerLedger(){
        return new Object[][]{
            {5},
            {MAX_ENTRY_COUNT_PER_LEDGER}
        };
    }

    @Test(dataProvider = "entryCountPerLedger")
    public void testBacklog(int entryCountPerLedger) throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName = "sub1";
        int ledgerCount = 5;
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        SendInfo sendInfo1 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName,
                getManagedCursor(topicName, subName).getMessagesConsumedCounter());
        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(false),
                sendInfo1.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(true),
                sendInfo1.messageIds.size());

        consumer.acknowledge(sendInfo1.messageIds);
        awaitForMarkDeletedPosition(topicName, subName, sendInfo1.lastMessageId.getLedgerId(),
                sendInfo1.lastMessageId.getEntryId());
        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(false), 0);
        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(true), 0);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName,
                getManagedCursor(topicName, subName).getMessagesConsumedCounter());

        trimLedger(topicName);

        SendInfo sendInfo2 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName,
                getManagedCursor(topicName, subName).getMessagesConsumedCounter());

        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(false),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName).getNumberOfEntriesInBacklog(true),
                sendInfo2.messageIds.size());

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "entryCountPerLedger")
    public void testBacklogIfCursorCreateAfterTrimLedger(int entryCountPerLedger) throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName1 = "sub1";
        String subName2 = "sub2";
        String subName3 = "sub3";
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName1).subscribe();
        Producer<String> producer =pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        int ledgerCount = 5;
        SendInfo sendInfo1 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());

        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName2).subscribe();

        consumer1.acknowledge(sendInfo1.messageIds);
        consumer2.acknowledge(sendInfo1.messageIds);
        awaitForMarkDeletedPosition(topicName, subName1, sendInfo1.lastMessageId.getLedgerId(),
                sendInfo1.lastMessageId.getEntryId());
        awaitForMarkDeletedPosition(topicName, subName2, sendInfo1.lastMessageId.getLedgerId(),
                sendInfo1.lastMessageId.getEntryId());
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName2,
                getManagedCursor(topicName, subName2).getMessagesConsumedCounter());

        trimLedger(topicName);

        SendInfo sendInfo2 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());

        Consumer<String> consumer3 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName3).subscribe();
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName2,
                getManagedCursor(topicName, subName2).getMessagesConsumedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName3,
                getManagedCursor(topicName, subName3).getMessagesConsumedCounter());

        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(false),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(true),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(false),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(true),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName3).getNumberOfEntriesInBacklog(false),
                sendInfo2.messageIds.size());
        assertEquals(getManagedCursor(topicName, subName3).getNumberOfEntriesInBacklog(true),
                sendInfo2.messageIds.size());

        // cleanup.
        producer.close();
        consumer1.close();
        consumer2.close();
        consumer3.close();
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 1000 * 3600)
    public void testBacklogIfCursorCreateAfterTrimLedger2() throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName1 = "sub1";
        String subName2 = "sub2";
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName1).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName2).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        int ledgerCount = 5;
        int entryCountPerLedger = MAX_ENTRY_COUNT_PER_LEDGER;
        SendInfo sendInfo1 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());

        CompletableFuture<Void> unloadFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                admin.topics().unload(topicName);
                unloadFuture.complete(null);
            } catch (Exception e) {
                unloadFuture.completeExceptionally(e);
            }
        }).start();

        CompletableFuture<SendInfo> send2Future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                send2Future.complete(sendMessages(1, entryCountPerLedger, producer, topicName));
            } catch (Exception e) {
                send2Future.completeExceptionally(e);
            }
        }).start();

        unloadFuture.join();
        send2Future.join();
        // TODO debug 模式有重复写 Bookie 的 bug。
        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(false),
                sendInfo1.messageIds.size() + send2Future.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(true),
                sendInfo1.messageIds.size() + send2Future.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(false),
                sendInfo1.messageIds.size() + send2Future.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(true),
                sendInfo1.messageIds.size() + send2Future.join().messageIds.size());

        // cleanup.
        producer.close();
        consumer1.close();
        consumer2.close();
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 1000 * 3600)
    public void testBacklogIfCursorCreateAfterTrimLedger3() throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName1 = "sub1";
        String subName2 = "sub2";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName1).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        CompletableFuture<SendInfo> sendFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                sendFuture.complete(sendMessages(1, 1, producer, topicName));
            } catch (Exception e) {
                sendFuture.completeExceptionally(e);
            }
        }).start();

        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName2).subscribe();
        sendFuture.join();

        int backlog = 0;
        PositionImpl sub2MarkDeleted = (PositionImpl) getManagedCursor(topicName, subName2).getMarkDeletedPosition();
        for (MessageId msgId : sendFuture.join().messageIds){
            MessageIdImpl msgIdImpl = (MessageIdImpl) msgId;
            if (msgIdImpl.getLedgerId() > sub2MarkDeleted.getLedgerId()){
                backlog++;
            } else if (msgIdImpl.getLedgerId() == sub2MarkDeleted.getLedgerId()){
                if (msgIdImpl.getEntryId() > sub2MarkDeleted.getEntryId()){
                    backlog++;
                }
            }
        }

        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(false),
                sendFuture.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(true),
                sendFuture.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(false), backlog);
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(true), backlog);

        assertEquals(getManagedCursor(topicName, subName2).messagesConsumedCounter, 0);

        // cleanup.
        producer.close();
        consumer1.close();
        consumer2.close();
        admin.topics().delete(topicName, false);
    }
}
