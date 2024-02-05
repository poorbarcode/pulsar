/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.lookup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.common.naming.TopicName;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class GetTopicsResult {
    @Getter
    @Setter
    private List<String> topics;
    @Getter
    @Setter
    private String topicsHash;
    @Getter
    @Setter
    private boolean filtered;
    @Getter
    @Setter
    private boolean changed;
    /** This variable will be initialized in Constructor. **/
    @JsonIgnore
    private Map<String, GroupedTopic> groupedTopics;

    public GetTopicsResult(List<String> topics, String topicsHash, boolean filtered, boolean changed) {
        this.topics = topics;
        this.topicsHash = topicsHash;
        this.filtered = filtered;
        this.changed = changed;
        Map<String, GroupedTopic> groupedTopics = new LinkedHashMap<>();
        for (String t : topics) {
            final TopicName topicName = TopicName.get(t);
            GroupedTopic groupedTopic = groupedTopics.computeIfAbsent(topicName.getPartitionedTopicName(), k -> {
                GroupedTopic newOne = new GroupedTopic();
                if (topicName.isPartitioned()) {
                    newOne.isNonPartitioned = false;
                    newOne.partitions = new ArrayList<>();
                } else {
                    newOne.isNonPartitioned = true;
                    newOne.partitions = Collections.emptyList();
                }
                return newOne;
            });
            if (!groupedTopic.isNonPartitioned) {
                groupedTopic.partitions.add(topicName.getPartitionIndex());
            }
        }
    }

    public List<String> getGroupedTopicNames() {
        return new ArrayList<>(groupedTopics.keySet());
    }

    private class GroupedTopic {

    }
}
