package io.streamnative.streamingmetastore.api.messages;

import io.streamnative.streamingmetastore.api.KeyValue;
import lombok.Data;

@Data
public class WatchEvent {
    private final KeyValue current;
    private final KeyValue prev;
    private final EventType type;

    public enum EventType {
        PUT,
        DELETE,
        UNKNOWN
    }
}
