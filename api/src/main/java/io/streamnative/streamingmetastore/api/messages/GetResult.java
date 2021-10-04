package io.streamnative.streamingmetastore.api.messages;

import io.streamnative.streamingmetastore.api.KeyValue;
import lombok.Data;

@Data
public class GetResult {
    private final ResultHeader header;
    private final KeyValue keyValue;
}
