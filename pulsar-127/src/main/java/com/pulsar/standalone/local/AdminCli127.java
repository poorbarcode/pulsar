package com.pulsar.standalone.local;

import org.apache.pulsar.admin.cli.PulsarAdminTool;

public class AdminCli127 {

    public static void main(String[] args) throws Exception {
        PathExtractor.INSTANCE.initEnv(true);
        String[] pulsarArgs = new String[]{
            PathExtractor.INSTANCE.clientConfPath,
            "clusters", "list"
        };
        PulsarAdminTool.main(pulsarArgs);
    }
}
