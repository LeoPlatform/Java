package com.leo.sdk;

import com.leo.sdk.payload.EventPayload;

import javax.inject.Singleton;

@Singleton
public interface AsyncWorkQueue {
    void addEntity(EventPayload entity);

    StreamStats end();

    TransferStyle style();
}
