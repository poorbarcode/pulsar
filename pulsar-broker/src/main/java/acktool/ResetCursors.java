package acktool;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class ResetCursors {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            print("pulsar-reset-cursor.sh <topics-and-missing-ledgers.json> <auth-token>");
            System.exit(0);
        }

        String brokerUrl = "pulsar://pulsar-proxy:6650";
        String webUrl = "http://pulsar-proxy:8080";
        String token = args[1];

        // Create client.
        final PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl)
                .authentication(AuthenticationFactory.token(token)).build();
        final PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(webUrl)
                .authentication(AuthenticationFactory.token(token)).build();
        Scanner scanner = new Scanner(System.in);

        // Read lost ledgers from file.
        Map<String, List<String>> lostLedgers = readLostLedgers(args[0]);

        // Loop topics.
        for (Map.Entry<String, List<String>> e : lostLedgers.entrySet()) {
            try {
                Set<Long> ledgersInFile = new LinkedHashSet<>();
                for (String l : e.getValue()) {
                    ledgersInFile.add(Long.valueOf(l));
                }

                String topicName = String.format("persistent://%s", e.getKey());

                print("===> Start work on topic " + e.getKey());

                // Call topics stats.
                TopicStats stats = admin.topics().getStats(topicName);
                PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName);

                // Check should skip this topic.
                Map<String, ? extends SubscriptionStats> subs = stats.getSubscriptions();
                if (subs == null || subs.isEmpty()) {
                    print("    -> Skip topic: there has no subscriptions. " + e.getKey());
                    continue;
                }

                if (internalStats.ledgers == null || internalStats.ledgers.isEmpty()) {
                    print("    -> Skip topic: there has no ledgers. " + e.getKey());
                    continue;
                }

                long largestLedgerId = -1;
                Map<Long, ManagedLedgerInternalStats.LedgerInfo> ledgerMap = new LinkedHashMap<>();
                for (ManagedLedgerInternalStats.LedgerInfo ledgerInfo : internalStats.ledgers) {
                    if (ledgersInFile.contains(Long.valueOf(ledgerInfo.ledgerId))) {
                        ledgerMap.put(ledgerInfo.ledgerId, ledgerInfo);
                        largestLedgerId = ledgerInfo.ledgerId;
                    }
                }

                if (largestLedgerId < 0) {
                    print("    -> Skip topic: can not find lost ledgers in topic.ledgers. " + topicName
                            + ", lost ledgers: " + ledgersInFile.toString());
                    print("    -> topic.ledgers: " + internalStats.ledgers.stream().map(i -> i.ledgerId).collect(
                            Collectors.toList()));
                    continue;
                }
                print("    -> lost ledgers. " + ledgerMap.entrySet().stream()
                        .map(i -> i.getKey() + ":0~" + (i.getValue().entries - 1))
                        .collect(Collectors.toList()).toString());

                // Loop subscriptions.
                for (Map.Entry<String, ? extends SubscriptionStats> s : subs.entrySet()) {
                    String subName = s.getKey();

                    print("    ===> Start work on subscription " + subName);

                    SubscriptionStats subStat = s.getValue();
                    ManagedLedgerInternalStats.CursorStats cursorStats = internalStats.cursors.get(subName);
                    SubscriptionType subType = SubscriptionType.valueOf(subStat.getType());
                    String[] markDeletedPos = cursorStats.markDeletePosition.split(":");

                    print("    -> Sub type " + subType.toString());

                    Long markDeletedLedgerId = Long.valueOf(markDeletedPos[0]);
                    Long markDeletedEntryId = Long.valueOf(markDeletedPos[1]);
                    print("    -> mark delete position  " + markDeletedLedgerId + ":" + markDeletedEntryId);

                    List<Long> individual = Arrays.stream(cursorStats.individuallyDeletedMessages
                                    .replaceAll("\\(", "")
                                    .replaceAll("\\)", "")
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .split(","))
                            .map(str -> str.split(":")[0])
                            .filter(str -> !StringUtils.isBlank(str))
                            .map(str -> Long.valueOf(str))
                            .collect(Collectors.toList());
                    print("    -> individual ACK ledgers  " + individual.toString());

                    if (markDeletedLedgerId > largestLedgerId) {
                        print("    <=== Skip subscription: mark deleted position is larger than the max lost ledger "
                                + largestLedgerId + ". " + e.getKey() + " " + subName);
                        continue;
                    }

                    // Collect the message-ids which should be acknowledged and print a pre-log.
                    List<MessageIdImpl> allMessageIds = new ArrayList<>();
                    for (ManagedLedgerInternalStats.LedgerInfo ledgerInfo : ledgerMap.values()){
//                        long l = ledgerInfo.ledgerId;
//                        if (!individualSet.contains(Long.valueOf(l))){
//                            print("        -> Skip ledger: the lost ledger is not in individual ACK records. " + l);
//                            continue;
//                        }
                        List<MessageIdImpl> messageIds =
                                collectMessageIdsAndPrintPreLog(topicName, subName, markDeletedLedgerId,
                                        markDeletedEntryId, ledgerInfo);
                        allMessageIds.addAll(messageIds);
                    }

                    // Let the user confirm whether to acknowledge messages.
                    print("    -> Do you want to acknowledge these " + allMessageIds.size()
                            + " entries? Please type Y/N/Exit");
                    String confirm = scanner.next();
                    if ("Y".equalsIgnoreCase(confirm)) {
                        print("    -> create zero queue consumer."
                                + " {ackReceipt: true,"
                                + " sub-type: " + subType + "}");
                        Consumer consumer = client.newConsumer().topic(topicName).subscriptionName(subName)
                                .subscriptionType(subType).receiverQueueSize(0)
                                .isAckReceiptEnabled(true).subscribe();
                        consumer.acknowledge(allMessageIds);
                        consumer.close();
                        print("    -> acknowledge finished " + subType);
                        print("    -> try confirm the mark delete position ");
                        String newMarkDeletedPosition =
                                admin.topics().getInternalStats(topicName).cursors.get(subName).markDeletePosition;
                        print("    -> new mark delete position: " + newMarkDeletedPosition);
                        if (cursorStats.markDeletePosition.equalsIgnoreCase(newMarkDeletedPosition)) {
                            print("    -> ERROR the mark deleted position has not moved, exit this progress");
                            System.exit(0);
                        } else {
                            print("    <=== Finished work on subscription " + subName);
                            print("    <=== You can also call \"bin/pulsar-admin topics stats-internal " + topicName
                                    + "\" to confirm the result.");
                        }
                    } else if ("Exit".equalsIgnoreCase(confirm)) {
                        print("<=== Exit");
                        System.exit(0);
                    } else {
                        print("    <=== Skip subscription " + subName);
                    }
                }
            } finally {
                print("<=== Finished work on " + e.getKey());
                print("");
            }
        }

        scanner.close();
        client.close();
        admin.close();
        System.exit(0);
    }

    private static List<MessageIdImpl> collectMessageIdsAndPrintPreLog(String topicName, String subName,
                                                                       long markDeletedLedgerId,
                                                                       long markDeletedEntryId,
                                                                       ManagedLedgerInternalStats.LedgerInfo lostLedger){
        TopicName tp = TopicName.get(topicName);
        List<MessageIdImpl> ids = new ArrayList<>();
        if (lostLedger.ledgerId < markDeletedLedgerId || lostLedger.entries < 1) {
            print("        -> Skip ledger: mark delete position is larger than the lost ledger.  "
                    + lostLedger.ledgerId);
            return ids;
        } else if (lostLedger.entries < 1) {
            print("        -> Skip ledger: the lost ledger is an empty ledger.  " + lostLedger.ledgerId);
            return ids;
        } else if (lostLedger.ledgerId == markDeletedLedgerId) {
            print(String.format("        -> In the lost ledger %s, these entries will be acknowledged: %s~%s",
                    lostLedger.ledgerId, markDeletedEntryId + 1, lostLedger.entries - 1));
            for (long e = markDeletedEntryId + 1; e < lostLedger.entries; e++){
                MessageIdImpl messageId = new MessageIdImpl(lostLedger.ledgerId, e, tp.getPartitionIndex());
                ids.add(messageId);
            }
        } else {
            print(String.format("        -> In the lost ledger %s, these entries will be acknowledged: %s~%s",
                    lostLedger.ledgerId, 0, lostLedger.entries - 1));
            for (long e = 0; e < lostLedger.entries; e++){
                MessageIdImpl messageId = new MessageIdImpl(lostLedger.ledgerId, e, tp.getPartitionIndex());
                ids.add(messageId);
            }
        }
        return ids;
    }

    private static Map<String, List<String>> readLostLedgers(String filePath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, List<String>>> data = objectMapper.readValue(new File(filePath), Map.class);
        return data.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get("ledgers")));
    }

    /**
     * Easy to change the log way.
     */
    private static void print(String line) {
        System.out.println(line);
    }
}
