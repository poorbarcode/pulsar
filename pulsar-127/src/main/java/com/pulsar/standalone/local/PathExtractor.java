package com.pulsar.standalone.local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class PathExtractor {

    public static final PathExtractor INSTANCE = new PathExtractor();

    public final String pulsarHome;

    public final String pulsarConfHome;

    public final String pulsar127Home;

    public final String log4JConfPath;

    public final String pulsarJaasConfPath;

    public final String krb5ConfPath;

    public final String brokerConfPath;

    public final String standaloneConfPath;

    public final String proxyConfPath;

    public final String clientConfPath;

    public final String zookeeperConfPath;

    public final String webSocketConfPath;

    public final String bookkeeperConfPath;

    private PathExtractor() {
        // log4j.
        log4JConfPath = Thread.currentThread().getContextClassLoader().getResource("log4j2.xml").getFile();
        String pulsar127Classes = new File(log4JConfPath).getParent();
        String pulsar127Target = new File(pulsar127Classes).getParent();
        String kerberosConfigDir = new File(pulsar127Classes, "kerberos").getAbsolutePath();
        // pulsar home.
        pulsar127Home = new File(pulsar127Target).getParent();
        pulsarHome = new File(pulsar127Home).getParent();
        pulsarConfHome = new File(pulsarHome, "conf").getAbsolutePath();
        // kerberos.
        pulsarJaasConfPath = new File(kerberosConfigDir, "pulsar_jaas.conf").getAbsolutePath();
        krb5ConfPath = new File(kerberosConfigDir, "krb5.conf").getAbsolutePath();
        // conf for starter.
        brokerConfPath = new File(pulsarConfHome, "broker.conf").getAbsolutePath();
        standaloneConfPath = new File(pulsarConfHome, "standalone.conf").getAbsolutePath();
        proxyConfPath = new File(pulsarConfHome, "proxy.conf").getAbsolutePath();
        clientConfPath = new File(pulsarConfHome, "client.conf").getAbsolutePath();
        zookeeperConfPath = new File(pulsarConfHome, "zookeeper.conf").getAbsolutePath();
        webSocketConfPath = new File(pulsarConfHome, "websocket.conf").getAbsolutePath();
        bookkeeperConfPath = new File(pulsarConfHome, "bookkeeper.conf").getAbsolutePath();
    }

    private void rewriteJaasConf(String pulsarJaasConfPath) throws IOException {
        String kerberosConfigDir = new File(pulsarJaasConfPath).getParent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(pulsarJaasConfPath)));
        List<String> lines = new ArrayList<>();
        while (true){
            String line = reader.readLine();
            if (line == null){
                break;
            }
            lines.add(line.replaceAll("KERBEROS_CONF_DIR", kerberosConfigDir));
        }
        reader.close();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(pulsarJaasConfPath, false)
        ));
        for (String line : lines){
            writer.println(line);
        }
        writer.flush();
        writer.close();
    }

    public void initEnv(boolean jaas) {
        // log4j.
        System.setProperty("log4j2.is.webapp", "false");
        System.setProperty("log4j.configurationFile", log4JConfPath);
        // file encode.
        System.setProperty("file.encoding", "UTF-8");
        // netty
        System.setProperty("io.netty.leakDetectionLevel", "disabled");
        // kerberos
        if (jaas) {
            try {
                rewriteJaasConf(pulsarJaasConfPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.setProperty("java.security.auth.login.config", pulsarJaasConfPath);
            System.setProperty("java.security.krb5.conf", krb5ConfPath);
        }
    }
}
