package com.leo.sdk;

import com.leo.sdk.payload.SimplePayload;
import com.leo.sdk.payload.StreamPayload;

public interface WriteQueue {
    void addEntity(SimplePayload entity);

    void addEntity(StreamPayload entity);

    StreamStats end();

    Uploader type();
}
