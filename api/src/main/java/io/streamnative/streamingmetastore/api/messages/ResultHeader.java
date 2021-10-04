package io.streamnative.streamingmetastore.api.messages;

import lombok.Data;

@Data
public class ResultHeader {
    private final long term;
    private final long revision;
}
