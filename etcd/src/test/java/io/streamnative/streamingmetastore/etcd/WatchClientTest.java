package io.streamnative.streamingmetastore.etcd;

import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.api.WatchClient;
import io.streamnative.streamingmetastore.test.AbstractWatchClientTest;

public class WatchClientTest extends AbstractWatchClientTest {
    @Override
    protected KVClient createKVClient() throws Exception {
        return new ETCDMetaStore("http://localhost:2379");
    }

    @Override
    protected WatchClient createWatchClient() throws Exception {
        return new ETCDMetaStore("http://localhost:2379");
    }
}
