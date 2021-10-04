package io.streamnative.streamingmetastore.api;

public interface EventObserver<T> {
    void onNext(T event);

    void onError(Throwable throwable);

    void onCompleted();
}
