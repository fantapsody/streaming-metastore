package io.streamnative.streamingmetastore.api.messages;

import lombok.Data;

@Data
public class GrantResult {
    private final ResultHeader header;
    private final long leaseId;
    private final long ttl;
}
