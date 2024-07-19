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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;

public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        final AtomicInteger atomicInteger = new AtomicInteger();
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://127.0.0.1:6661")
                .enableTls(true)
                .tlsTrustCertsFilePath("/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/ca.cert.pem")
                .enableTlsHostnameVerification(false)
                .allowTlsInsecureConnection(false)
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                        "tlsCertFile:/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/client.cert.pem," +
                                "tlsKeyFile:/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/client.key-pk8.pem")
                .build();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("my-topic")
                .enableBatching(false)
                .batchingMaxMessages(2)
                .messageRouter(new MessageRouter(){
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return atomicInteger.incrementAndGet() % metadata.numPartitions() + 1;
                    }
                })
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .create();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 100; i++){
            String messageSuffix = String.format("%s-%s", timestamp, i);
            producer.newMessage()
                    .key(String.valueOf(timestamp + i))
                    .value(String.format("My message-%s", messageSuffix).getBytes())
                    .send();
        }
    }
}
