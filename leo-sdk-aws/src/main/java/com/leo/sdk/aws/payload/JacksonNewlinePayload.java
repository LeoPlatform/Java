package com.leo.sdk.aws.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.StreamJsonPayload;

import java.io.IOException;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

public final class JacksonNewlinePayload implements StreamJsonPayload {
    private static final String NEWLINE = "\n";

    private final ObjectMapper mapper = buildMapper();

    @Override
    public String toJsonString(EntityPayload entityPayload) {
        try {
            String json = mapper.writeValueAsString(entityPayload);
            return String.format("%s%s", json, NEWLINE);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create JSON payload");
        }
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .setSerializationInclusion(ALWAYS)
                .registerModule(new JSR353Module());
    }
}
