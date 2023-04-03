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

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class SimpleConsumer {

    public static void main(String[] args) throws Exception{
        PulsarClient client = PulsarClient.builder()
                .operationTimeout(30000, TimeUnit.SECONDS)
                .serviceUrl("pulsar://127.0.0.1:6650")
                .build();
        Consumer consumer = client.newConsumer(Schema.BYTES)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscribe();
        while (true) {
            // Wait for a message
            Message msg = consumer.receive();
            try {
                // Do something with the message
                MessageId messageIdData = msg.getMessageId();
                System.out.println(messageIdToString(msg.getMessageId()) + "Message received: " + new String(msg.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
                System.out.println("Message ack: " + messageIdToString(msg.getMessageId()));
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }

    public static String messageIdToString(MessageId messageId){
        if (messageId instanceof BatchMessageIdImpl){
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl)messageId;
            return String.format("ledgerId: %s, entryId: %s, batchSize: %s, batchIndex: %s, acker: %s",
                    batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                    batchMessageId.getOriginalBatchSize(), batchMessageId.getBatchIndex(),
                    batchMessageId.getAcker().toString());
        } else if (messageId instanceof MessageIdImpl){
            MessageIdImpl messageIdImpl = (MessageIdImpl)messageId;
            return String.format("ledgerId: %s, entryId: %s",
                    messageIdImpl.getLedgerId(), messageIdImpl.getEntryId());
        }
        return String.valueOf(messageId);
    }
}
