package com.pulsar.standalone.local;

import org.apache.pulsar.proxy.server.ProxyServiceStarter;

public class Proxy127 {

    public static void main(String[] args) throws Exception {
        PathExtractor.INSTANCE.initEnv(false);
        String[] pulsarArgs = new String[]{
            "--config", PathExtractor.INSTANCE.proxyConfPath
        };
        ProxyServiceStarter.main(pulsarArgs);
    }
}
