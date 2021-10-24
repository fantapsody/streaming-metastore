package io.streamnative.streamingmetastore.api;

import java.util.concurrent.CompletableFuture;

public interface IDGenerator {
    CompletableFuture<Long> generateId(String group);
}
