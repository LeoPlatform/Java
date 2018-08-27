package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.StreamJsonPayload;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class JSDKGzipPayload implements PayloadCompression {

    private final StreamJsonPayload streamJson;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public JSDKGzipPayload(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        this.streamJson = streamJson;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public ByteBuffer compress(EntityPayload payload) {
        String json = streamJson.toJsonString(payload);
        byte[] gzipped = toGzip(json);
        thresholdMonitor.addBytes(gzipped.length);
        return ByteBuffer.wrap(gzipped);
    }

    private byte[] toGzip(String json) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(512)) {
            toStream(json, byteStream);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Could not serialize compressed payload", e);
        }
    }

    private void toStream(String json, ByteArrayOutputStream byteStream) {
        try (OutputStream os = new GZIPOutputStream(byteStream, true)) {
            os.write(json.getBytes(UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Could not compress payload", e);
        }
    }
}
