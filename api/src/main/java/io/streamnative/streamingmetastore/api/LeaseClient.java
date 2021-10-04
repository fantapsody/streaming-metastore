package io.streamnative.streamingmetastore.api;

import io.streamnative.streamingmetastore.api.messages.GrantResult;
import io.streamnative.streamingmetastore.api.messages.KeepAliveResult;
import io.streamnative.streamingmetastore.api.messages.RevokeResult;
import java.util.concurrent.CompletableFuture;

public interface LeaseClient extends AutoCloseable {
    CompletableFuture<GrantResult> grant(long ttl);

    CompletableFuture<RevokeResult> revoke(long leaseId);

    AutoCloseable keepAlive(long leaseId, EventObserver<KeepAliveResult> observer);
}
