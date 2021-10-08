package io.streamnative.streamingmetastore.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.streamnative.streamingmetastore.api.ByteSeq;
import io.streamnative.streamingmetastore.api.EventObserver;
import io.streamnative.streamingmetastore.api.KVClient;
import io.streamnative.streamingmetastore.api.WatchClient;
import io.streamnative.streamingmetastore.api.messages.DeleteResult;
import io.streamnative.streamingmetastore.api.messages.GetRangeResult;
import io.streamnative.streamingmetastore.api.messages.WatchEvent;
import io.streamnative.streamingmetastore.api.messages.WatchResult;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWatchClientTest {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractWatchClientTest.class);

    protected abstract KVClient createKVClient() throws Exception;

    protected abstract WatchClient createWatchClient() throws Exception;

    @Test
    void basic() throws Exception {
        try (KVClient kvClient = createKVClient()) {
            try (WatchClient watchClient = createWatchClient()) {
                int count = 10;
                String keyPrefix = "/test-";
                String valuePrefix = "value-";
                String newValuePrefix = "newValue-";
                WatchEventCollector collector = new WatchEventCollector();
                WatchClient.Watcher watcher = watchClient.watch(ByteSeq.from(keyPrefix), collector,
                        WatchClient.WatchOption.builder()
                                .prefix(true)
                                .prevKV(true)
                                .build());
                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    String value = valuePrefix + i;
                    assertNotNull(kvClient.put(ByteSeq.from(key), ByteSeq.from(value), null).join());
                }
                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    String value = valuePrefix + i;
                    WatchEvent event = collector.getQueue().poll(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(event);
                    assertEquals(WatchEvent.EventType.PUT, event.getType());
                    assertTrue(event.getPrev().isEmpty());
                    assertEquals(key, event.getCurrent().getKey().toString());
                    assertEquals(value, event.getCurrent().getValue().toString());
                }

                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    String value = newValuePrefix + i;
                    assertNotNull(kvClient.put(ByteSeq.from(key), ByteSeq.from(value), null).join());
                }
                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    String value = valuePrefix + i;
                    String newValue = newValuePrefix + i;
                    WatchEvent event = collector.getQueue().poll(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(event);
                    assertEquals(WatchEvent.EventType.PUT, event.getType());
                    assertEquals(key, event.getPrev().getKey().toString());
                    assertEquals(value, event.getPrev().getValue().toString());
                    assertEquals(key, event.getCurrent().getKey().toString());
                    assertEquals(newValue, event.getCurrent().getValue().toString());
                }

                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    assertNotNull(kvClient.delete(ByteSeq.from(key), null).join());
                }
                for (int i = 0; i < count; i++) {
                    String key = keyPrefix + i;
                    String value = newValuePrefix + i;
                    WatchEvent event = collector.getQueue().poll(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(event);
                    assertEquals(WatchEvent.EventType.DELETE, event.getType());
                    assertEquals(key, event.getPrev().getKey().toString());
                    assertEquals(value, event.getPrev().getValue().toString());
                    assertEquals(key, event.getCurrent().getKey().toString());
                    assertTrue(event.getCurrent().getMetaData().getModRevision() > event.getPrev().getMetaData().getModRevision());
                }
                assertNull(collector.getQueue().poll(100, TimeUnit.MILLISECONDS));
                watcher.close();
            }
        }
    }

    @Test
    public void startRevision() throws Exception {
        try (KVClient kvClient = createKVClient()) {
            try (WatchClient watchClient = createWatchClient()) {
                int count = 10;
                String keyPrefix = "/test-";
                String valuePrefix = "value-";
                WatchEventCollector collector = new WatchEventCollector();
                try (WatchClient.Watcher watcher = watchClient.watch(ByteSeq.from(keyPrefix), collector,
                        WatchClient.WatchOption.builder()
                                .prefix(true)
                                .prevKV(true)
                                .build())) {
                    for (int i = 0; i < count; i++) {
                        String key = keyPrefix + i;
                        String value = valuePrefix + i;
                        assertNotNull(kvClient.put(ByteSeq.from(key), ByteSeq.from(value), null).join());
                    }
                    GetRangeResult getRangeResult = kvClient
                            .getRange(ByteSeq.from("/"), ByteSeq.from("/").increasedLastByte(), null).join();
                    assertEquals(count, getRangeResult.getKeyValueList().size());
                    WatchEventCollector collector1 = new WatchEventCollector();
                    long startRevision = -1;
                    for (int i = 0; i < count; i++) {
                        String key = keyPrefix + i;
                        DeleteResult delResult = kvClient.delete(ByteSeq.from(key), null).join();
                        assertNotNull(delResult);
                        if (startRevision < 0) {
                            startRevision = delResult.getHeader().getRevision();
                        }
                    }
                    try (WatchClient.Watcher watcher1 = watchClient.watch(ByteSeq.from(keyPrefix), collector1,
                            WatchClient.WatchOption.builder()
                                    .prefix(true)
                                    .prevKV(true)
                                    .revision(startRevision)
                                    .build())) {

                        for (int i = 0; i < count; i++) {
                            String key = keyPrefix + i;
                            String value = valuePrefix + i;
                            WatchEvent event = collector.getQueue().poll(1000, TimeUnit.MILLISECONDS);
                            assertNotNull(event);

                            assertEquals(WatchEvent.EventType.PUT, event.getType());
                            assertEquals(key, event.getCurrent().getKey().toString());
                            assertEquals(value, event.getCurrent().getValue().toString());
                        }

                        for (int i = 0; i < count; i++) {
                            String key = keyPrefix + i;
                            String value = valuePrefix + i;
                            WatchEvent event = collector1.getQueue().poll(1000, TimeUnit.MILLISECONDS);
                            assertNotNull(event);
                            assertEquals(WatchEvent.EventType.DELETE, event.getType());
                            assertEquals(key, event.getPrev().getKey().toString());
                            assertEquals(value, event.getPrev().getValue().toString());
                            assertEquals(key, event.getCurrent().getKey().toString());
                            assertTrue(event.getCurrent().getMetaData().getModRevision() > event.getPrev().getMetaData().getModRevision());
                        }
                    }
                }
            }
        }
    }

    static class WatchEventCollector implements EventObserver<WatchResult> {
        private final BlockingQueue<WatchEvent> queue = new LinkedBlockingDeque<>();

        @Override
        public void onNext(WatchResult event) {
            queue.addAll(event.getEvents());
        }

        @Override
        public void onError(Throwable throwable) {
            fail(throwable);
        }

        @Override
        public void onCompleted() {
            logger.info("Watch completed");
        }

        public BlockingQueue<WatchEvent> getQueue() {
            return queue;
        }
    }
}
