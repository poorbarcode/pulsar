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
package com.pulsar.standalone.local;

import io.streamnative.cloud.plugins.interceptor.RestAPIInterceptor;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.pulsar.PulsarStandaloneStarter;

@Slf4j
public class PulsarStandaloneWithPlugin {

    /***
     * JVM args:
     *   -Dlog4j2.is.webapp=false -Dfile.encoding=UTF-8 -Dlog4j.configurationFile=${LOG4J_CONFIG}/local_log4j2.xml.
     * Process args:
     *   --no-functions-worker --config ${PULSAR_HOME}/conf/standalone.conf.
     */
    public static void main(String[] args) throws Exception {
        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        try {
            standalone.getConfig().setDisableBrokerInterceptors(false);
            standalone.getConfig().setWithPlugin(true);
            Properties properties = new Properties();
            properties.setProperty("clusterName", "standalone");
            properties.setProperty("snCloudRestApiInterceptorConfigFile",
                    PulsarStandaloneWithPlugin.class.getClassLoader()
                            .getResource("admin_interceptor.json").getPath());
            standalone.getConfig().setProperties(properties);
            standalone.start();
        } catch (Throwable th) {
            log.error("Failed to start pulsar service.", th);
            LogManager.shutdown();
            Runtime.getRuntime().exit(1);
        }
    }
}
