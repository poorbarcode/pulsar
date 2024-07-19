package test.org.apache.pulsar.auth;

import org.apache.pulsar.client.api.PulsarClient;

public class TlsKeyStoreClientTool {

    public static PulsarClient buildClient() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://127.0.0.1:6651")
                .useKeyStoreTls(true)
                .tlsTrustStorePath("/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tls_key_store/client.truststore.jks")
                .tlsTrustStorePassword("clientpw")
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(false)
                .authentication(
                        "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls",
                        "keyStoreType:JKS," +
                                "keyStorePath:/home/fengyubiao/Data/Git-Reposirtory/fork/pulsar/pulsar-127/src/main/resources/tls_key_store/client.keystore.jks," +
                                "keyStorePassword:clientpw")
                .build();
        return client;
    }
}
