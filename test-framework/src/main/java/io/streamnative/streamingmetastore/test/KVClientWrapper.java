package io.streamnative.streamingmetastore.test;

import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import io.streamnative.streamingmetastore.api.messages.PutResult;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class KVClientWrapper implements KVClient {
    private final KVClient client;

    public KVClientWrapper(KVClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(ByteSeq key, GetOptions options) {
        return client.get(key, options);
    }

    @Override
    public CompletableFuture<GetRangeResult> getRange(ByteSeq start, ByteSeq end, GetRangeOptions options) {
        return client.getRange(start, end, options);
    }

    @Override
    public CompletableFuture<PutResult> put(ByteSeq key, ByteSeq value, PutOptions options) {
        return client.put(key, value, options);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(ByteSeq key, DeleteOptions options) {
        return client.delete(key, options);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
