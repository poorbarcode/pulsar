package com.pulsar.standalone.local;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationSasl;

public class AdminRest127 {

    public static void main(String[] args) throws Exception {
        String brokerUrl = "http://localhost:18080";
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl);

        PathExtractor.INSTANCE.initEnv(true);
        Map<String, String> authParams = new HashMap<>();
        authParams.put("saslJaasClientSectionName", "PulsarClient");
        authParams.put("serverType", "proxy");
        Authentication saslAuth = AuthenticationFactory.create(AuthenticationSasl.class.getName(), authParams);
        pulsarAdminBuilder.authentication(saslAuth);

        PulsarAdmin admin = pulsarAdminBuilder.build();
//        admin.topics().createNonPartitionedTopic("persistent://public/default/tp1");

        admin.clusters().getClusters();
        admin.clusters().getClusters();
        admin.clusters().getClusters();
        System.exit(0);
    }
}
