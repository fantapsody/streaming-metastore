package io.streamnative.streamingmetastore.api;

import lombok.Data;

@Data
public class KeyValue {
    public static KeyValue EMPTY = new KeyValue(new KeyValueMetaData(0, 0, 0), ByteSeq.EMPTY, ByteSeq.EMPTY);

    private final KeyValueMetaData metaData;
    private final ByteSeq key;
    private final ByteSeq value;

    public boolean isEmpty() {
        return this.equals(EMPTY);
    }
}
