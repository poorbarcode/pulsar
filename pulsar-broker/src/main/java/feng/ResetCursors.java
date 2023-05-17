package feng;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
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
        if (args.length != 1) {
            print("pulsar-reset-cursor.sh <topics-and-missing-ledgers.json>");
            System.exit(0);
        }

        String brokerUrl = "pulsar://pulsar-proxy:6650";
        String webUrl = "http://pulsar-proxy:8080";
        // Please rewrite the token.
        String token = "token of client";

        final PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl)
            .authentication(AuthenticationFactory.token(token)).build();
        final PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(webUrl)
                .authentication(AuthenticationFactory.token(token)).build();

        Map<String, List<Long>> lostLedgers = readLostLedgers(args[0]);

        for (Map.Entry<String, List<Long>> e : lostLedgers.entrySet()) {
            String topicName = String.format("persistent://%s", e.getKey());

            print("===> Start work on topic " + e.getKey());

            TopicStats stats = admin.topics().getStats(topicName);
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName);

            Map<String, ? extends SubscriptionStats> subs = stats.getSubscriptions();
            if (subs == null || subs.isEmpty()){
                print("    -> Skip topic: there has no subscriptions. " + e.getKey());
                continue;
            }

            if (internalStats.ledgers == null || internalStats.ledgers.isEmpty()){
                print("    -> Skip topic: there has no ledgers. " + e.getKey());
                continue;
            }

            long largestLedgerId = -1;
            Map<Long, ManagedLedgerInternalStats.LedgerInfo> ledgerMap = new LinkedHashMap<>();
            for (ManagedLedgerInternalStats.LedgerInfo ledgerInfo : internalStats.ledgers) {
                if (e.getValue().contains(ledgerInfo.ledgerId)) {
                    ledgerMap.put(ledgerInfo.ledgerId, ledgerInfo);
                    largestLedgerId = ledgerInfo.ledgerId;
                }
            }

            if (largestLedgerId < 0) {
                print("    -> Skip topic: can not find lost ledgers in topic. " + e.getKey() + "  "
                        + e.getValue().toString());
                continue;
            }
            print("    -> lost ledgers. " + ledgerMap.entrySet().stream()
                    .map(i -> i.getKey() + ":0-" + i.getValue().entries).collect(Collectors.toList()).toString());

            for (Map.Entry<String, ? extends SubscriptionStats> s : subs.entrySet()){
                String subName = s.getKey();

                print("    -> Start work on subscription " + subName);

                SubscriptionStats subStat = s.getValue();
                ManagedLedgerInternalStats.CursorStats cursorStats = internalStats.cursors.get(subName);
                SubscriptionType subType = SubscriptionType.valueOf(subStat.getType());
                String[] markDeletedPos = cursorStats.markDeletePosition.split(":");

                print("    -> Sub type " + subType.toString());

                Long markDeletedLedgerId = Long.valueOf(markDeletedPos[0]);
                Long markDeletedEntryId = Long.valueOf(markDeletedPos[0]);
                print("    -> mark delete position  " + markDeletedLedgerId + ":" + markDeletedEntryId);

                List<Long> individual = Arrays.stream(cursorStats.individuallyDeletedMessages
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", "")
                        .replaceAll("\\[", "")
                        .replaceAll("\\]", "")
                        .split(",")).map(str -> Long.valueOf(str.split(":")[0]))
                        .collect(Collectors.toList());
                print("    -> individual ACK ledgers  " + individual.toString());

                if (markDeletedLedgerId > largestLedgerId) {
                    print("    -> Skip subscription: mark deleted position is larger than the max lost ledger "
                            + largestLedgerId + ". " + e.getKey() + " " + subName);
                }

                for (int i = individual.size() - 1; i >= 0; i--) {
                    long l = individual.get(i);
                    ManagedLedgerInternalStats.LedgerInfo ledgerInfo = ledgerMap.get(l);
                    if (ledgerInfo == null) {
                        print("        -> Skip ledger: the lost ledger is not in individual ACK records.  " + l);
                    }
                    if (ledgerMap.containsKey(l)) {
                        List<MessageIdImpl> messageIds = calculateMessageIds(topicName, subName, markDeletedLedgerId,
                                markDeletedEntryId, ledgerInfo);
                    }
                }
            }
            print("<=== Finished work on " + e.getKey());
            print("");
        }

        client.close();
        admin.close();
        System.exit(0);
    }

    private static List<MessageIdImpl> calculateMessageIds(String topicName, String subName, long markDeletedLedgerId,
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
            print(String.format("        -> %s dry run acknowledge %s:%s~%s:%s", subName, lostLedger.ledgerId,
                    markDeletedEntryId + 1, lostLedger.ledgerId, lostLedger.entries - 1));
            for (long e = markDeletedEntryId + 1; e < lostLedger.entries; e++){
                MessageIdImpl messageId = new MessageIdImpl(lostLedger.ledgerId, e, tp.getPartitionIndex());
                ids.add(messageId);
            }
        } else {
            print(String.format("        -> %s dry run acknowledge %s:%s~%s:%s", subName, lostLedger.ledgerId, 0,
                    lostLedger.ledgerId, lostLedger.entries - 1));
            for (long e = 0; e < lostLedger.entries; e++){
                MessageIdImpl messageId = new MessageIdImpl(lostLedger.ledgerId, e, tp.getPartitionIndex());
                ids.add(messageId);
            }
        }
        return ids;
    }

    private static Map<String, List<Long>> readLostLedgers(String filePath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, List<Long>>> data = objectMapper.readValue(new File(filePath), Map.class);
        return data.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get("ledgers")));
    }

    private static void print(String line) {
        System.out.println(line);
    }
}
