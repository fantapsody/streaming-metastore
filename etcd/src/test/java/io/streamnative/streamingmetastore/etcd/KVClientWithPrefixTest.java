package io.streamnative.streamingmetastore.etcd;

import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.test.AbstractKVClientTest;

public class KVClientWithPrefixTest extends AbstractKVClientTest {
    @Override
    protected KVClient createKVClient() throws Exception {
        return new ETCDMetaStore("http://docker.internal:2379", ByteSeq.from("/root"));
    }
}
