package com.leo.sdk.payload;

import javax.json.JsonObject;
import java.time.Instant;

public interface SimplePayload {
    default Instant eventTime() {
        return Instant.now();
    }

    JsonObject entity();
}
