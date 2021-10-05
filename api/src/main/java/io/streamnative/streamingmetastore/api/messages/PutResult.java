package io.streamnative.streamingmetastore.api.messages;

import io.streamnative.streamingmetastore.api.KeyValueMetaData;
import lombok.Data;

@Data
public class PutResult {
    private final ResultHeader header;
    private final KeyValueMetaData keyValueMetaData;
}
