package io.streamnative.streamingmetastore.api.messages;

import io.streamnative.streamingmetastore.api.KeyValue;
import java.util.List;
import lombok.Data;

@Data
public class GetRangeResult {
    private final ResultHeader header;
    private final List<KeyValue> keyValueList;
}
