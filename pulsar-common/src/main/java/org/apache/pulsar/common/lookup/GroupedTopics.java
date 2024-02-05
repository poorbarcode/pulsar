package org.apache.pulsar.common.lookup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.common.naming.TopicName;

public class GroupedTopics {

    private final Map<String, GroupedTopic> topicMap = new HashMap<>();

    /***
     * @param topicNamesWithPartition topic names contains partition suffix.
     */
    public GroupedTopics(List<String> topicNamesWithPartition) {
        for (String topicNameWithPartition : topicNamesWithPartition) {
            addTopic(topicNameWithPartition);
        }
    }

    /***
     * @param topicNameWithPartition topic name contains partition suffix.
     */
    public void addTopic(String topicNameWithPartition) {
        final TopicName topicName = TopicName.get(topicNameWithPartition);
        GroupedTopic groupedTopic = topicMap.computeIfAbsent(topicName.getPartitionedTopicName(), k -> {
            GroupedTopic newOne = new GroupedTopic();
            if (topicName.isPartitioned()) {
                newOne.nonPartitionTopic = false;
                newOne.partitions = new ArrayList<>();
            } else {
                newOne.nonPartitionTopic = true;
                newOne.partitions = Collections.emptyList();
            }
            return newOne;
        });
        if (!groupedTopic.nonPartitionTopic) {
            groupedTopic.partitions.add(topicName.getPartitionIndex());
        }
    }

    /***
     * @param topicNameWithPartition topic name contains partition suffix.
     * @return true if removed an item.
     */
    public boolean removeTopic(String topicNameWithPartition) {
        final TopicName topicName = TopicName.get(topicNameWithPartition);
        if (!topicName.isPartitioned()) {
            return topicMap.remove(topicName.getPartitionedTopicName()) != null;
        }
        GroupedTopic groupedTopic = topicMap.get(topicName.getPartitionedTopicName());
        if (groupedTopic == null) {
            return false;
        }
        if (!groupedTopic.partitions.contains(topicName.getPartitionIndex())) {
            return false;
        }
        groupedTopic.partitions.remove(topicName.getPartitionIndex());
        return true;
    }

    @Data
    public static class GroupedTopic {
        private String name;
        private boolean nonPartitionTopic;
        private List<Integer> partitions;
    }
}