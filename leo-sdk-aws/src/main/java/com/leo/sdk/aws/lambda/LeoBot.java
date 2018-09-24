package com.leo.sdk.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.leo.sdk.DaggerSDKPlatform;
import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.stream.Stream;

public final class LeoBot implements RequestStreamHandler {
    private static final Logger log = LoggerFactory.getLogger(LeoBot.class);

    @Override
    public final void handleRequest(InputStream input, OutputStream output, Context context) {

        ConnectorConfig config = DaggerSDKPlatform.builder()
                .build()
                .connectorConfig();
        LambdaExecution botExecution = fromClass(config.value("Bot"));
        try {
            botExecution.run(context);
        } catch (Exception e) {
            log.error("Unhandled exception in bot execution", e);
            throw new IllegalStateException("Unhandled bot error", e);
        }
    }

    private LambdaExecution fromClass(String name) {
        try {
            String n = Optional.ofNullable(name)
                    .orElseThrow(() -> new IllegalArgumentException("'Bot' parameter required"));

            @SuppressWarnings("unchecked")
            Class<LambdaExecution> cl = (Class<LambdaExecution>) Class.forName(n).asSubclass(LambdaExecution.class);

            return (LambdaExecution) Stream
                    .of(cl.getDeclaredConstructors())
                    .filter(c -> c.getParameterCount() == 0)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(name + " must have a no-arg constructor"))
                    .newInstance();

        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid 'Bot' parameter: " + name, e);
        }
    }
}
