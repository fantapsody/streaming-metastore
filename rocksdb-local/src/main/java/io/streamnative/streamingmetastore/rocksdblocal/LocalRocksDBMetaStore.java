package io.streamnative.streamingmetastore.rocksdblocal;

import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.api.KeyValue;
import io.streamnative.streamingmetastore.api.KeyValueMetaData;
import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import io.streamnative.streamingmetastore.api.messages.PutResult;
import io.streamnative.streamingmetastore.api.messages.ResultHeader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

public class LocalRocksDBMetaStore implements Closeable, KVClient {
    static {
        RocksDB.loadLibrary();
    }

    private final TransactionDB db;

    public LocalRocksDBMetaStore(String path) throws RocksDBException {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final TransactionDBOptions transactionDBOptions = new TransactionDBOptions()) {
                this.db = TransactionDB.open(options, transactionDBOptions, path);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.db != null) {
            this.db.close();
        }
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(ByteSeq key, GetOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                byte[] value = this.db.get(key.getBytes());
                if (value == null) {
                    return Optional.empty();
                } else {
                    return Optional.of(new GetResult(new ResultHeader(0, 0),
                            new KeyValue(new KeyValueMetaData(0, 0, 0),
                                    key, ByteSeq.from(value))));
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<GetRangeResult> getRange(ByteSeq start, ByteSeq end, GetRangeOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try (RocksIterator iter = this.db.newIterator(new ReadOptions()
                    .setIterateLowerBound(new Slice(start.getBytes()))
                    .setIterateUpperBound(new Slice(end.getBytes())));) {
                iter.seekToFirst();
                List<KeyValue> data = new ArrayList<>();
                while (iter.isValid()) {
                    data.add(new KeyValue(new KeyValueMetaData(0, 0, 0),
                            ByteSeq.from(iter.key()), ByteSeq.from(iter.value())));
                    iter.next();
                }
                return new GetRangeResult(new ResultHeader(0, 0), data);
            }
        });
    }

    @Override
    public CompletableFuture<PutResult> put(ByteSeq key, ByteSeq value, PutOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                this.db.put(key.getBytes(), value.getBytes());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return new PutResult(new ResultHeader(0, 0), new KeyValueMetaData(0, 0, 0));
        });
    }

    @Override
    public CompletableFuture<DeleteResult> delete(ByteSeq key, DeleteOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                this.db.delete(key.getBytes());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return new DeleteResult(new ResultHeader(0, 0));
        });
    }
}
