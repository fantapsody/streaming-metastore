package io.streamnative.streamingmetastore.api;

import lombok.Data;

@Data
public class KeyValueMetaData {
    private final long createRevision;
    private final long modRevision;
    private final long version;
}
