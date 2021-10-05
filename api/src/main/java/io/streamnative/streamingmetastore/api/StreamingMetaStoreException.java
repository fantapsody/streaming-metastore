package io.streamnative.streamingmetastore.api;

public class StreamingMetaStoreException extends RuntimeException {

    public StreamingMetaStoreException(String message) {
        super(message);
    }

    public static class BadVersionException extends StreamingMetaStoreException {
        public BadVersionException() {
            super("bad version");
        }
    }
}
