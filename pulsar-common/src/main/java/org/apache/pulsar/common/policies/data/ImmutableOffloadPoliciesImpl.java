package org.apache.pulsar.common.policies.data;

import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ImmutableOffloadPoliciesImpl {

    private OffloadPoliciesImpl delegate;

    public String getOffloadersDirectory() {
        return delegate.getOffloadersDirectory();
    }

    public String getManagedLedgerOffloadDriver() {
        return delegate.getManagedLedgerOffloadDriver();
    }

    public Integer getManagedLedgerOffloadMaxThreads() {
        return delegate.getManagedLedgerOffloadMaxThreads();
    }

    public Integer getManagedLedgerOffloadPrefetchRounds() {
        return delegate.getManagedLedgerOffloadPrefetchRounds();
    }

    public Long getManagedLedgerOffloadThresholdInSeconds() {
        return delegate.getManagedLedgerOffloadThresholdInSeconds();
    }

    public Long getManagedLedgerOffloadThresholdInBytes() {
        return delegate.getManagedLedgerOffloadThresholdInBytes();
    }

    public Long getManagedLedgerOffloadDeletionLagInMillis() {
        return delegate.getManagedLedgerOffloadDeletionLagInMillis();
    }

    /**
     * @return {@link OffloadedReadPriority} is a enum, so just return it.
     */
    public OffloadedReadPriority getManagedLedgerOffloadedReadPriority() {
        return delegate.getManagedLedgerOffloadedReadPriority();
    }

    public Map<String, String> getManagedLedgerExtraConfigurations() {
        return Collections.unmodifiableMap(delegate.getManagedLedgerExtraConfigurations());
    }

    public String getS3ManagedLedgerOffloadRegion() {
        return delegate.getS3ManagedLedgerOffloadRegion();
    }

    public String getS3ManagedLedgerOffloadBucket() {
        return delegate.getS3ManagedLedgerOffloadBucket();
    }

    public String getS3ManagedLedgerOffloadServiceEndpoint() {
        return delegate.getS3ManagedLedgerOffloadServiceEndpoint();
    }

    public Integer getS3ManagedLedgerOffloadMaxBlockSizeInBytes() {
        return delegate.getS3ManagedLedgerOffloadMaxBlockSizeInBytes();
    }

    public Integer getS3ManagedLedgerOffloadReadBufferSizeInBytes() {
        return delegate.getS3ManagedLedgerOffloadReadBufferSizeInBytes();
    }

    public String getS3ManagedLedgerOffloadCredentialId() {
        return delegate.getS3ManagedLedgerOffloadCredentialId();
    }

    public String getS3ManagedLedgerOffloadCredentialSecret() {
        return delegate.getS3ManagedLedgerOffloadCredentialSecret();
    }

    public String getS3ManagedLedgerOffloadRole() {
        return delegate.getS3ManagedLedgerOffloadRole();
    }

    public String getS3ManagedLedgerOffloadRoleSessionName() {
        return delegate.getS3ManagedLedgerOffloadRoleSessionName();
    }

    public String getGcsManagedLedgerOffloadRegion() {
        return delegate.getGcsManagedLedgerOffloadRegion();
    }

    public String getGcsManagedLedgerOffloadBucket() {
        return delegate.getGcsManagedLedgerOffloadBucket();
    }

    public Integer getGcsManagedLedgerOffloadMaxBlockSizeInBytes() {
        return delegate.getGcsManagedLedgerOffloadMaxBlockSizeInBytes();
    }

    public Integer getGcsManagedLedgerOffloadReadBufferSizeInBytes() {
        return delegate.getGcsManagedLedgerOffloadReadBufferSizeInBytes();
    }

    public String getGcsManagedLedgerOffloadServiceAccountKeyFile() {
        return delegate.getGcsManagedLedgerOffloadServiceAccountKeyFile();
    }

    public String getFileSystemProfilePath() {
        return delegate.getFileSystemProfilePath();
    }

    public String getFileSystemURI() {
        return delegate.getFileSystemURI();
    }

    public String getManagedLedgerOffloadBucket() {
        return delegate.getManagedLedgerOffloadBucket();
    }

    public String getManagedLedgerOffloadRegion() {
        return delegate.getManagedLedgerOffloadRegion();
    }

    public String getManagedLedgerOffloadServiceEndpoint() {
        return delegate.getManagedLedgerOffloadServiceEndpoint();
    }

    public Integer getManagedLedgerOffloadMaxBlockSizeInBytes() {
        return delegate.getManagedLedgerOffloadMaxBlockSizeInBytes();
    }

    public Integer getManagedLedgerOffloadReadBufferSizeInBytes() {
        return delegate.getManagedLedgerOffloadReadBufferSizeInBytes();
    }
}
