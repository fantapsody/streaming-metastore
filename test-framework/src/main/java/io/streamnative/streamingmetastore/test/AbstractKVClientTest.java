package io.streamnative.streamingmetastore.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.api.KeyValue;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.GetResult;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class AbstractKVClientTest {
    protected abstract KVClient createKVClient() throws Exception;

    @Test
    void basic() throws Exception {
        try (KVClient client = createKVClient()) {
            int count = 10;
            String keyPrefix = "/test-";
            String valuePrefix = "value-";
            String dirPrefix = "/test-dir/";
            for (int i = 0; i < count; i++) {
                String key = keyPrefix + i;
                String value = valuePrefix + i;
                assertNotNull(client.put(ByteSeq.from(key), ByteSeq.from(value), null).join());
                Optional<GetResult> getResult = client.get(ByteSeq.from(key), null).join();
                assertTrue(getResult.isPresent());
                assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), getResult.get().getKeyValue().getValue().getBytes());

                key = dirPrefix + i;
                assertNotNull(client.put(ByteSeq.from(key), ByteSeq.from(value), null).join());
                getResult = client.get(ByteSeq.from(key), null).join();
                assertTrue(getResult.isPresent());
                assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), getResult.get().getKeyValue().getValue().getBytes());
            }

            GetRangeResult getRangeResult = client.getRange(ByteSeq.from("/"), ByteSeq.from("/").increasedLastByte(), null).join();
            assertEquals(count * 2, getRangeResult.getKeyValueList().size());

            getRangeResult = client.getRange(ByteSeq.from(dirPrefix), ByteSeq.from(dirPrefix).increasedLastByte(), null).join();
            assertEquals(count, getRangeResult.getKeyValueList().size());
            for (int i = 0; i < getRangeResult.getKeyValueList().size(); i++) {
                String key = dirPrefix + i;
                String value = valuePrefix + i;
                KeyValue kv = getRangeResult.getKeyValueList().get(i);
                assertEquals(key, kv.getKey().toString());
                assertEquals(value, kv.getValue().toString());
            }

            for (int i = 0; i < count; i++) {
                String key = keyPrefix + i;
                assertNotNull(client.delete(ByteSeq.from(key), null).join());
                Optional<GetResult> getResult = client.get(ByteSeq.from(key), null).join();
                assertFalse(getResult.isPresent());
            }
        }
    }
}
