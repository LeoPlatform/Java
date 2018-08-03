package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.payload.StreamJsonPayload;
import com.leo.sdk.payload.StreamPayload;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JSDKGzipPayload implements KinesisCompression {

    private final StreamJsonPayload streamJson;

    @Inject
    public JSDKGzipPayload(StreamJsonPayload streamJson) {
        this.streamJson = streamJson;
    }

    @Override
    public ByteBuffer compress(StreamPayload payload) {
        String json = streamJson.toJsonString(payload);
        return ByteBuffer.wrap(toGzip(json));
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
        try (OutputStream os = new GZIPOutputStream(byteStream)) {
            os.write(json.getBytes(UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Could not compress payload", e);
        }
    }
}
