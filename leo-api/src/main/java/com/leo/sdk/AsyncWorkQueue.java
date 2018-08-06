package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;

public interface AsyncWorkQueue {
    void addEntity(SimplePayload entity);

    void addEntity(EntityPayload entity);

    StreamStats end();

    TransferStyle style();
}
