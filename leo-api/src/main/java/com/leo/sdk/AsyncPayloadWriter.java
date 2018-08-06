package com.leo.sdk;

public interface AsyncPayloadWriter {
    void write(PayloadIdentifier payload);

    StreamStats end();

    TransferStyle style();
}
