package io.streamnative.streamingmetastore.rocksdblocal;

import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.test.AbstractKVClientTest;
import io.streamnative.streamingmetastore.test.IOUtils;
import io.streamnative.streamingmetastore.test.KVClientWrapper;
import java.nio.file.Files;
import java.nio.file.Path;

public class KVClientTest extends AbstractKVClientTest {
    @Override
    protected KVClient createKVClient() throws Exception {
        final Path dir = Files.createTempDirectory("local_rocksdb");
        return new KVClientWrapper(new LocalRocksDBMetaStore(dir.toString())) {
            @Override
            public void close() throws Exception {
                super.close();
                IOUtils.deletePath(dir);
            }
        };
    }
}
