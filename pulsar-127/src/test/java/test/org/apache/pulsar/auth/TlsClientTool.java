package test.org.apache.pulsar.auth;

import org.apache.pulsar.client.api.PulsarClient;

public class TlsClientTool {

    public static PulsarClient buildClient() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://127.0.0.1:6651")
                .enableTls(true)
                .tlsTrustCertsFilePath("/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/ca.cert.pem")
                .enableTlsHostnameVerification(false)
                .allowTlsInsecureConnection(false)
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                        "tlsCertFile:/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/client.cert.pem," +
                                "tlsKeyFile:/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tsl/client.key-pk8.pem")
                .build();
        return client;
    }
}
