/**
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
package test.org.apache.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;

@Slf4j
public class TransactionConsumerAndProducer {

    public static void main(String[] args) throws Exception{
        // Build pulsar client
        PulsarClient client = PulsarClient.builder()
                .operationTimeout(30000, TimeUnit.SECONDS)
                .serviceUrl("pulsar://127.0.0.1:6650")
                .enableTransaction(true)
                .build();
        // create batch consumer
        Consumer consumer = client.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .topic("my-topic")
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionName("my-subscription")
                .subscribe();
        // create producer
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("my-topic")
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .batchingMaxMessages(2)
                .create();

        while (true) {
            // start transaction
            log.info("start transaction");
            Transaction transaction =
                    client.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            // Receive
            log.info("receive message");
            Message msg = consumer.receive();
            System.out.println(SimpleConsumer.messageIdToString(msg.getMessageId()) + " Message received: " + new String(
                    msg.getData()));
            // Send
            log.info("send message");
            producer.newMessage(transaction)
                    .value(new StringBuilder("tx message 0-").append(String.valueOf(msg.getMessageId())).toString()
                            .getBytes(
                                    StandardCharsets.UTF_8)).sendAsync();
            producer.newMessage(transaction)
                    .value(new StringBuilder("tx message 1-").append(String.valueOf(msg.getMessageId())).toString()
                            .getBytes(
                                    StandardCharsets.UTF_8)).sendAsync();
            // Acknowledge
            log.info("acknowledge");
            consumer.acknowledgeAsync(msg.getMessageId(), transaction);
            // commit
            log.info("commit");
            transaction.commit().get();
            // exit
            log.info("finish");
            Thread.sleep(1000);
        }
    }
}
