package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EventPayload;

import javax.inject.Singleton;

@Singleton
public interface AsyncWorkQueue {
    void addEntity(EventPayload entity);

    void flush();

    StreamStats end();

    TransferStyle style();
}
