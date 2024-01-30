package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

public abstract class ImmutableTopicPolicies implements Cloneable {

    public TopicPolicies cloneAsMutable() {
        TopicPolicies topicPolicies = (TopicPolicies) this;
        return ObjectUtils.clone(topicPolicies);
    }

    public abstract boolean isGlobalPolicies();

    public abstract Boolean getDeduplicationEnabled();

    public abstract Integer getMessageTTLInSeconds();

    public abstract Integer getMaxProducerPerTopic();

    public abstract Integer getMaxConsumerPerTopic();

    public abstract Integer getMaxConsumersPerSubscription();

    public abstract Integer getMaxUnackedMessagesOnConsumer();

    public abstract Integer getMaxUnackedMessagesOnSubscription();

    public abstract Long getDelayedDeliveryTickTimeMillis();

    public abstract Boolean getDelayedDeliveryEnabled();

    public abstract Boolean getDispatcherPauseOnAckStatePersistentEnabled();

    public abstract Long getCompactionThreshold();

    public abstract Integer getDeduplicationSnapshotIntervalSeconds();

    public abstract Integer getMaxMessageSize();

    public abstract Integer getMaxSubscriptionsPerTopic();

    public abstract Boolean getSchemaValidationEnforced();


    // --- The methods that returns an immutable object

    /**
     * @return {@link SchemaCompatibilityStrategy} is a enum, so just return it.
     */
    public abstract SchemaCompatibilityStrategy getSchemaCompatibilityStrategy();

    /**
     * @return {@link PersistencePolicies} is immutable, so just return it.
     */
    public abstract PersistencePolicies getPersistence();

    /**
     * @return {@link RetentionPolicies} is immutable, so just return it.
     */
    public abstract RetentionPolicies getRetentionPolicies();

    /**
     * @return {@link InactiveTopicPolicies} is immutable, so just return it.
     */
    public abstract InactiveTopicPolicies getInactiveTopicPolicies();

    /**
     * @return {@link DispatchRateImpl} is immutable, so just return it.
     */
    public abstract DispatchRateImpl getDispatchRate();

    /**
     * @return {@link DispatchRateImpl} is immutable, so just return it.
     */
    public abstract DispatchRateImpl getSubscriptionDispatchRate();

    /**
     * @return {@link PublishRate} is immutable, so just return it.
     */
    public abstract PublishRate getPublishRate();

    /**
     * @return {@link SubscribeRate} is immutable, so just return it.
     */
    public abstract SubscribeRate getSubscribeRate();

    /**
     * @return {@link DispatchRateImpl} is immutable, so just return it.
     */
    public abstract DispatchRateImpl getReplicatorDispatchRate();

    /**
     * @return {@link EntryFilters} is immutable, so just return it.
     */
    public abstract EntryFilters getEntryFilters();

    /**
     * @return {@link AutoSubscriptionCreationOverrideImpl} is immutable, so just return it.
     */
    public abstract AutoSubscriptionCreationOverrideImpl getAutoSubscriptionCreationOverride();


    // --- The methods that name "getImmutable---".

    @JsonIgnore
    public Map<String, BacklogQuotaImpl> getImmutableBackLogQuotaMap() {
        Map<String, BacklogQuotaImpl> src = getBackLogQuotaMap();
        if (src == null) {
            return null;
        }
        return Collections.unmodifiableMap(src);
    }

    @JsonIgnore
    public List<CommandSubscribe.SubType> getImmutableSubscriptionTypesEnabled() {
        List<CommandSubscribe.SubType> src = getSubscriptionTypesEnabled();
        if (src == null) {
            return null;
        }
        return Collections.unmodifiableList(src);
    }

    @JsonIgnore
    public List<String> getImmutableReplicationClusters() {
        List<String> src = getReplicationClusters();
        if (src == null) {
            return null;
        }
        return Collections.unmodifiableList(src);
    }

    @JsonIgnore
    public List<String> getImmutableShadowTopics() {
        List<String> src = getShadowTopics();
        if (src == null) {
            return null;
        }
        return Collections.unmodifiableList(src);
    }

    @JsonIgnore
    public Map<String, ImmutableSubscriptionPolicies> getImmutableSubscriptionPolicies() {
        Map<String, SubscriptionPolicies> src = getSubscriptionPolicies();
        if (src == null) {
            return null;
        }
        return Collections.unmodifiableMap(src.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> new ImmutableSubscriptionPolicies(e.getValue()))));
    }

    @JsonIgnore
    public ImmutableOffloadPoliciesImpl getImmutableOffloadPolicies() {
        return new ImmutableOffloadPoliciesImpl(getOffloadPolicies());
    }

    protected abstract Map<String, BacklogQuotaImpl> getBackLogQuotaMap();

    protected abstract List<CommandSubscribe.SubType> getSubscriptionTypesEnabled();

    protected abstract List<String> getReplicationClusters();

    protected abstract List<String> getShadowTopics();

    protected abstract Map<String, SubscriptionPolicies> getSubscriptionPolicies();

    protected abstract OffloadPoliciesImpl getOffloadPolicies();


    // ------ The methods that are not Getter|Setter.

    @JsonIgnore
    public abstract boolean isReplicatorDispatchRateSet();

    @JsonIgnore
    public abstract boolean isMaxSubscriptionsPerTopicSet();

    @JsonIgnore
    public abstract boolean isMaxMessageSizeSet();

    @JsonIgnore
    public abstract boolean isDeduplicationSnapshotIntervalSecondsSet();

    @JsonIgnore
    public abstract boolean isInactiveTopicPoliciesSet();

    @JsonIgnore
    public abstract boolean isOffloadPoliciesSet();

    @JsonIgnore
    public abstract boolean isMaxUnackedMessagesOnConsumerSet();

    @JsonIgnore
    public abstract boolean isDelayedDeliveryTickTimeMillisSet();

    @JsonIgnore
    public abstract boolean isDelayedDeliveryEnabledSet();

    @JsonIgnore
    public abstract boolean isMaxUnackedMessagesOnSubscriptionSet();

    @JsonIgnore
    public abstract boolean isBacklogQuotaSet();

    @JsonIgnore
    public abstract boolean isPersistentPolicySet();

    @JsonIgnore
    public abstract boolean isRetentionSet();

    @JsonIgnore
    public abstract boolean isDeduplicationSet();

    @JsonIgnore
    public abstract boolean isMessageTTLSet();

    @JsonIgnore
    public abstract boolean isMaxProducerPerTopicSet();

    @JsonIgnore
    public abstract boolean isMaxConsumerPerTopicSet();

    @JsonIgnore
    public abstract boolean isMaxConsumersPerSubscriptionSet();

    @JsonIgnore
    public abstract boolean isDispatchRateSet();

    @JsonIgnore
    public abstract boolean isSubscriptionDispatchRateSet();

    @JsonIgnore
    public abstract boolean isCompactionThresholdSet();

    @JsonIgnore
    public abstract boolean isPublishRateSet();

    @JsonIgnore
    public abstract boolean isSubscribeRateSet();
}