package io.streamnative.streamingmetastore.api;

import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import io.streamnative.streamingmetastore.api.messages.PutResult;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Data;

public interface KVClient extends AutoCloseable {
    CompletableFuture<Optional<GetResult>> get(ByteSeq key, GetOptions options);

    CompletableFuture<GetRangeResult> getRange(ByteSeq start, ByteSeq end, GetRangeOptions options);

    CompletableFuture<PutResult> put(ByteSeq key, ByteSeq value, PutOptions options);

    CompletableFuture<DeleteResult> delete(ByteSeq key, DeleteOptions options);

    @Data
    class GetOptions {
    }

    @Data
    class GetRangeOptions {
    }

    @Data
    class PutOptions {
        long expectedRevision;
    }

    @Data
    class DeleteOptions {
        String endKey;
        long expectedRevision;
    }

    static String prefixEndForDirectory(String key) {
        if (key == null || key.isEmpty()) {
            return key;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        bytes[bytes.length - 1] += 1;
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
