package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.StreamJsonPayload;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class JSDKGzipWriter implements CompressionWriter {

    private final StreamJsonPayload streamJson;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public JSDKGzipWriter(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        this.streamJson = streamJson;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public void compress(Collection<EventPayload> payloads) {
//        String json = streamJson.toJsonString(payload);
//        byte[] gzipped = toGzip(json);
//        thresholdMonitor.addBytes(gzipped.length);
//        ByteBuffer b = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
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
