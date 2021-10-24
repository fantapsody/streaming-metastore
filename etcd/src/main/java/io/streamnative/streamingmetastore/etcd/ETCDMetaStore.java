package io.streamnative.streamingmetastore.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.stub.StreamObserver;
import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.EventObserver;
import io.streamnative.streamingmetastore.api.KeyValue;
import io.streamnative.streamingmetastore.api.KeyValueMetaData;
import io.streamnative.streamingmetastore.api.StreamingMetaStoreClient;
import io.streamnative.streamingmetastore.api.StreamingMetaStoreException;
import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import io.streamnative.streamingmetastore.api.messages.GrantResult;
import io.streamnative.streamingmetastore.api.messages.KeepAliveResult;
import io.streamnative.streamingmetastore.api.messages.PutResult;
import io.streamnative.streamingmetastore.api.messages.ResultHeader;
import io.streamnative.streamingmetastore.api.messages.RevokeResult;
import io.streamnative.streamingmetastore.api.messages.WatchEvent;
import io.streamnative.streamingmetastore.api.messages.WatchResult;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ETCDMetaStore implements StreamingMetaStoreClient {
    private final Client client;
    private final KV kvClient;
    private final Watch watchClient;
    private final Lease leaseClient;

    public ETCDMetaStore(String endpoints) {
        this.client = Client.builder()
                .endpoints(endpoints)
                .build();
        this.kvClient = client.getKVClient();
        this.watchClient = client.getWatchClient();
        this.leaseClient = client.getLeaseClient();
    }

    static KeyValue convert(io.etcd.jetcd.KeyValue kv) {
        if (kv.getCreateRevision() == 0 && kv.getKey().isEmpty() && kv.getValue().isEmpty()) {
            return KeyValue.EMPTY;
        }
        ByteSeq key = ByteSeq.from(kv.getKey().getBytes());
        ByteSeq value;
        if (kv.getValue() == null || kv.getValue().isEmpty()) {
            value = ByteSeq.EMPTY;
        } else {
            value = ByteSeq.from(kv.getValue().getBytes());
        }
        return new KeyValue(new KeyValueMetaData(kv.getCreateRevision(), kv.getModRevision(), kv.getVersion()),
                key, value);
    }

    static ResultHeader convert(Response.Header header) {
        return new ResultHeader(header.getRaftTerm(), header.getRevision());
    }

    static WatchEvent convert(io.etcd.jetcd.watch.WatchEvent event) {
        WatchEvent.EventType eventType;
        switch (event.getEventType()) {
            case PUT:
                eventType = WatchEvent.EventType.PUT;
                break;
            case DELETE:
                eventType = WatchEvent.EventType.DELETE;
                break;
            default:
                eventType = WatchEvent.EventType.UNKNOWN;
                break;
        }
        return new WatchEvent(convert(event.getKeyValue()), convert(event.getPrevKV()), eventType);
    }

    private static Watcher convert(Watch.Watcher watcher) {
        return watcher::close;
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(ByteSeq key, GetOptions options) {
        return this.kvClient.get(ByteSequence.from(key.getBytes()), GetOption.newBuilder().build())
                .thenApply(response -> {
                    if (response.getCount() == 0) {
                        return Optional.empty();
                    } else {
                        return Optional.of(new GetResult(convert(response.getHeader()),
                                convert(response.getKvs().get(0))));
                    }
                });
    }

    @Override
    public CompletableFuture<GetRangeResult> getRange(ByteSeq start, ByteSeq end, GetRangeOptions options) {
        GetOption.Builder builder = GetOption.newBuilder()
                .withSortField(GetOption.SortTarget.KEY)
                .withSortOrder(GetOption.SortOrder.ASCEND);
        if (end != null) {
            builder.withRange(ByteSequence.from(end.getBytes()));
        }
        if (options != null) {
            builder.withKeysOnly(options.isKeysOnly());
            builder.withLimit(options.getLimit());
        }

        return this.kvClient.get(ByteSequence.from(start.getBytes()), builder.build())
                .thenApply(response -> new GetRangeResult(convert(response.getHeader()),
                        response.getKvs().stream()
                                .map(ETCDMetaStore::convert)
                                .collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<PutResult> put(ByteSeq key, ByteSeq value, PutOptions options) {
        Txn txn;
        PutOption.Builder builder = PutOption.newBuilder();
        if (options != null && options.getLeaseId() != null) {
            builder.withLeaseId(options.getLeaseId());
        }
        if (options != null && options.getExpectedVersion() != null) {
            txn = this.kvClient.txn()
                    .If(new Cmp(ByteSequence.from(key.getBytes()),
                            Cmp.Op.EQUAL,
                            CmpTarget.version(options.getExpectedVersion())))
                    .Then(Op.put(ByteSequence.from(key.getBytes()), ByteSequence.from(value.getBytes()), builder.build()),
                            Op.get(ByteSequence.from(key.getBytes()), GetOption.DEFAULT))
                    .Else(Op.get(ByteSequence.from(key.getBytes()), GetOption.DEFAULT));
        } else {
            txn = this.kvClient.txn()
                    .Then(Op.put(ByteSequence.from(key.getBytes()), ByteSequence.from(value.getBytes()), builder.build()),
                            Op.get(ByteSequence.from(key.getBytes()), GetOption.DEFAULT));
        }
        return txn.commit().thenApply(response -> {
            if (response.isSucceeded()) {
                GetResponse get = response.getGetResponses().get(0);
                io.etcd.jetcd.KeyValue kv = get.getKvs().get(0);
                return new PutResult(convert(response.getHeader()),
                        new KeyValueMetaData(kv.getCreateRevision(), kv.getModRevision(), kv.getVersion()));
            } else {
                throw new StreamingMetaStoreException.BadVersionException();
            }
        });
    }

    @Override
    public CompletableFuture<DeleteResult> delete(ByteSeq key, DeleteOptions options) {
        Txn txn;
        if (options != null && options.getExpectedVersion() != null) {
            txn = this.kvClient.txn()
                    .If(new Cmp(ByteSequence.from(key.getBytes()),
                            Cmp.Op.EQUAL,
                            CmpTarget.version(options.getExpectedVersion())))
                    .Then(Op.delete(ByteSequence.from(key.getBytes()), DeleteOption.DEFAULT));
        } else {
            txn = this.kvClient.txn()
                    .Then(Op.delete(ByteSequence.from(key.getBytes()), DeleteOption.DEFAULT));
        }
        return txn.commit()
                .thenApply(txnResponse -> {
                    if (txnResponse.isSucceeded()) {
                        return new DeleteResult(convert(txnResponse.getHeader()));
                    } else {
                        throw new StreamingMetaStoreException.BadVersionException();
                    }
                });
    }

    @Override
    public CompletableFuture<GrantResult> grant(long ttl) {
        return this.leaseClient.grant(ttl)
                .thenApply(response -> new GrantResult(convert(response.getHeader()), response.getID(), response.getTTL()));
    }

    @Override
    public CompletableFuture<RevokeResult> revoke(long leaseId) {
        return this.leaseClient.revoke(leaseId)
                .thenApply(response -> new RevokeResult(convert(response.getHeader())));
    }

    @Override
    public AutoCloseable keepAlive(long leaseId, EventObserver<KeepAliveResult> observer) {
        return this.leaseClient.keepAlive(leaseId, new KeepAliveAdaptor(observer));
    }

    @Override
    public Watcher watch(ByteSeq key, EventObserver<WatchResult> listener, WatchOption option) {
        return convert(this.watchClient.watch(ByteSequence.from(key.getBytes()),
                convert(option), new EtcdWatcherListenerAdaptor(listener)));
    }

    private io.etcd.jetcd.options.WatchOption convert(WatchOption option) {
        if (option == null) {
            return null;
        }
        io.etcd.jetcd.options.WatchOption.Builder builder = io.etcd.jetcd.options.WatchOption.newBuilder();
        builder.isPrefix(option.isPrefix());
        builder.withPrevKV(option.isPrevKV());
        if (option.getRevision() != null) {
            builder.withRevision(option.getRevision());
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<Long> generateId(String group) {
        return put(ByteSeq.from("/ids/" + group), ByteSeq.EMPTY, null)
                .thenApply(res -> res.getKeyValueMetaData().getVersion());
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }

    static class EtcdWatcherListenerAdaptor implements Watch.Listener {
        private final EventObserver<WatchResult> observer;

        public EtcdWatcherListenerAdaptor(EventObserver<WatchResult> observer) {
            this.observer = observer;
        }

        @Override
        public void onNext(WatchResponse response) {
            WatchResult watchResult = new WatchResult(convert(response.getHeader()),
                    response.getEvents().stream()
                            .map(ETCDMetaStore::convert)
                            .collect(Collectors.toList()));
            observer.onNext(watchResult);
        }

        @Override
        public void onError(Throwable throwable) {
            observer.onError(throwable);
        }

        @Override
        public void onCompleted() {
            observer.onCompleted();
        }
    }

    static class KeepAliveAdaptor implements StreamObserver<LeaseKeepAliveResponse> {
        private final EventObserver<KeepAliveResult> observer;

        public KeepAliveAdaptor(EventObserver<KeepAliveResult> observer) {
            this.observer = observer;
        }

        @Override
        public void onNext(LeaseKeepAliveResponse response) {
            this.observer.onNext(new KeepAliveResult(convert(response.getHeader()), response.getID(), response.getTTL()));
        }

        @Override
        public void onError(Throwable t) {
            this.observer.onError(t);
        }

        @Override
        public void onCompleted() {
            this.observer.onCompleted();
        }
    }
}
