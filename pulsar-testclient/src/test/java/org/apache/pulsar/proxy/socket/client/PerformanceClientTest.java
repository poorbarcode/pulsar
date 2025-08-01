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
package org.apache.pulsar.proxy.socket.client;

import static org.testng.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class PerformanceClientTest {
    @Test(timeOut = 5000)
    public void testLoadArguments() throws Exception {
        PerformanceClient client = new PerformanceClient();

        // "--proxy-url" has the highest priority
        client.parse(getArgs("ws://broker0.pulsar.apache.org:8080/", "./src/test/resources/websocket_client1.conf"));
        client.loadArguments();
        assertEquals(client.proxyURL, "ws://broker0.pulsar.apache.org:8080/");

        // "webSocketServiceUrl" written in the conf file has the second priority
        client.parse(getArgs(null, "./src/test/resources/websocket_client1.conf"));
        client.loadArguments();
        assertEquals(client.proxyURL, "ws://broker1.pulsar.apache.org:8080/");

        // "webServiceUrl" written in the conf file has the third priority
        client.parse(getArgs(null, "./src/test/resources/websocket_client2.conf"));
        client.loadArguments();
        assertEquals(client.proxyURL, "ws://broker2.pulsar.apache.org:8080/");

        // "serviceUrl" written in the conf file has the fourth priority
        client.parse(getArgs(null, "./src/test/resources/websocket_client3.conf"));
        client.loadArguments();
        assertEquals(client.proxyURL, "wss://broker3.pulsar.apache.org:8443/");

        // The default value is "ws://localhost:8080/"
        client.parse(getArgs(null, null));
        client.loadArguments();
        assertEquals(client.proxyURL, "ws://localhost:8080/");

        // If the URL does not end with "/", it will be added
        client.parse(getArgs("ws://broker0.pulsar.apache.org:8080", null));
        client.loadArguments();
        assertEquals(client.proxyURL, "ws://broker0.pulsar.apache.org:8080/");
    }

    private String[] getArgs(String proxyUrl, String confFile) {
        List<String> args = new ArrayList<>();

        if (proxyUrl != null) {
            args.add("--proxy-url");
            args.add(proxyUrl);
        }

        if (confFile != null) {
            args.add("--conf-file");
            args.add(confFile);
        }

        args.add("persistent://public/default/dummy");
        return args.toArray(new String[args.size()]);
    }
}
