package io.streamnative.streamingmetastore.api;

import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import io.streamnative.streamingmetastore.api.messages.PutResult;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Data;

public interface KVClient extends AutoCloseable {
    CompletableFuture<Optional<GetResult>> get(ByteSeq key, GetOptions options);

    CompletableFuture<GetRangeResult> getRange(ByteSeq start, ByteSeq end, GetRangeOptions options);

    CompletableFuture<PutResult> put(ByteSeq key, ByteSeq value, PutOptions options);

    CompletableFuture<DeleteResult> delete(ByteSeq key, DeleteOptions options);

    @Data
    @Builder
    class GetOptions {
    }

    @Data
    @Builder
    class GetRangeOptions {
        private boolean keysOnly;
        private long limit;
    }

    @Data
    @Builder
    class PutOptions {
        private Long expectedVersion;
        private Long leaseId;
    }

    @Data
    @Builder
    class DeleteOptions {
        private String endKey;
        private Long expectedVersion;
    }
}
