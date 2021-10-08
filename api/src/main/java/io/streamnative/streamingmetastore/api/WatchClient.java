package io.streamnative.streamingmetastore.api;

import io.streamnative.streamingmetastore.api.messages.WatchResult;
import java.io.Closeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

public interface WatchClient extends AutoCloseable {
    Watcher watch(ByteSeq key, EventObserver<WatchResult> observer, WatchOption option);

    interface Watcher extends Closeable {
    }

    @Data
    @Builder
    @AllArgsConstructor
    class WatchOption {
        private final boolean prefix;
        private final boolean prevKV;
        private final Long revision;
    }
}
