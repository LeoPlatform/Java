package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;

public interface AsyncWorkQueue {
    void addEntity(EntityPayload entity);

    StreamStats end();

    TransferStyle style();
}
