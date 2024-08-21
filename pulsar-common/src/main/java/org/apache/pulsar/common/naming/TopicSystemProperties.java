package org.apache.pulsar.common.naming;

import java.util.Collections;
import java.util.Map;

public class TopicSystemProperties {

    public static final String CONTAINS_TXN_MESSAGES = "__contains_txn_messages";

    public static Map<String, String> containsTxnMessages(boolean value) {
        return Collections.singletonMap(CONTAINS_TXN_MESSAGES, String.valueOf(value));
    }
}