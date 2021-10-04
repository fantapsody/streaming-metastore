package io.streamnative.streamingmetastore.api;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface StreamingMetaStoreClient extends Closeable, KVClient, LeaseClient, WatchClient {
}
