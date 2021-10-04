package io.streamnative.streamingmetastore.api.messages;

import java.util.List;
import lombok.Data;

@Data
public class WatchResult {
    private final ResultHeader header;
    private final List<WatchEvent> events;
}
