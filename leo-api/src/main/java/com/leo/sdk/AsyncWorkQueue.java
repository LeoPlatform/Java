package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;

import javax.inject.Singleton;

@Singleton
public interface AsyncWorkQueue {
    void addEntity(EntityPayload entity);

    StreamStats end();

    TransferStyle style();
}
