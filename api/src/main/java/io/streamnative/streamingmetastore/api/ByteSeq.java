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

    public int size() {
        return bytes.length;
    }

    public ByteSeq append(ByteSeq seq) {
        byte[] newBytes = new byte[this.bytes.length + seq.bytes.length];
        System.arraycopy(this.bytes, 0, newBytes, 0, this.bytes.length);
        System.arraycopy(seq.bytes, 0, newBytes, this.bytes.length, seq.bytes.length);
        return new ByteSeq(newBytes);
    }

    public ByteSeq subSeq(int start) {
        return new ByteSeq(Arrays.copyOfRange(bytes, start, bytes.length));
    }

    public ByteSeq increasedLastByte() {
        if (isEmpty()) {
            return this;
        }
        byte[] bytes = getBytes();
        bytes[bytes.length - 1] += 1;
        return new ByteSeq(bytes);
    }

    @Override
    public String toString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
