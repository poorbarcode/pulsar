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

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;

public class SimpleBatchConsumer {

    public static void main(String[] args) throws Exception{
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        Consumer consumer = client.newConsumer()
                .topic("my-topic")
                .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(300).build())
                .subscriptionName("my-subscription")
                .subscribe();
        while (true) {
            // Wait for a message
            Messages messages = consumer.batchReceive();
            try {
                messages.forEach(msg -> {
                    // Do something with the message
                    System.out.println("Message received: " + String.valueOf(msg));
                });
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(messages);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(messages);
            }
        }
    }
}
