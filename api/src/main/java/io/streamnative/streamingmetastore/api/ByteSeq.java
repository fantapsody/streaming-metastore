package io.streamnative.streamingmetastore.api;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class ByteSeq {
    public static final ByteSeq EMPTY = new ByteSeq(new byte[0]);

    private final byte[] bytes;

    private ByteSeq(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes);
    }

    public static ByteSeq from(String string) {
        return new ByteSeq(string.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteSeq from(byte[] bytes) {
        return new ByteSeq(Arrays.copyOf(bytes, bytes.length));
    }

    public boolean isEmpty() {
        return this.bytes.length == 0;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public ByteSeq prefixEndForDirectory() {
        if (isEmpty()) {
            return this;
        }
        byte[] bytes = getBytes();
        bytes[bytes.length - 1] += 1;
        return from(bytes);
    }

    @Override
    public String toString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
