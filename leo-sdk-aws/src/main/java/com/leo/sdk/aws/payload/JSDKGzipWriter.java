package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.StreamJsonPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public final class JSDKGzipWriter implements CompressionWriter {
    private static final Logger log = LoggerFactory.getLogger(JSDKGzipWriter.class);

    private static final String NEWLINE = "\n";
    private final StreamJsonPayload streamJson;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public JSDKGzipWriter(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        this.streamJson = streamJson;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public ByteBuffer compress(Collection<EventPayload> payloads) {
        if (payloads.isEmpty()) {
            return ByteBuffer.wrap(new byte[0]);
        }
        String inflatedPayload = payloads.stream()
                .map(streamJson::toJsonString)
                .collect(Collectors.joining(NEWLINE));
        byte[] compressedPayload = toGzip(inflatedPayload);
        String plural = payloads.size() == 1 ? "" : "s";
        int inflatedLen = inflatedPayload.getBytes(UTF_8).length;
        int compressedLen = compressedPayload.length;
        log.info("Compression of {} payload{} from {} to {} bytes", payloads.size(), plural, inflatedLen, compressedLen);
        thresholdMonitor.addBytes(compressedLen);
        return ByteBuffer.wrap(compressedPayload);
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
