package io.streamnative.streamingmetastore.etcd;

import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.test.AbstractKVClientTest;

public class KVClientTest extends AbstractKVClientTest {
    @Override
    protected KVClient createKVClient() throws Exception {
        return new ETCDMetaStore("http://localhost:2379");
    }
}
