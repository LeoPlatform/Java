package com.leo.sdk.aws.payload;

import java.nio.ByteBuffer;

public class PayloadIdentifier {
    private final String id;
    private final ByteBuffer payload;

    public PayloadIdentifier(String id, ByteBuffer payload) {
        this.id = id;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public ByteBuffer getPayload() {
        return payload;
    }
}
