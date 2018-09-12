package com.leo.sdk.aws.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.StreamJsonPayload;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

@Singleton
public final class JacksonPayload implements StreamJsonPayload {
    private final ObjectMapper mapper = buildMapper();
    private final LoadingBot bot;

    @Inject
    public JacksonPayload(LoadingBot bot) {
        this.bot = bot;
    }

    @Override
    public String toJsonString(EventPayload eventPayload) {
        try {
            EntityPayload entityPayload = new EntityPayload(eventPayload, bot);
            return mapper.writeValueAsString(entityPayload);
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
