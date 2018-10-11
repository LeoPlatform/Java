package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.aws.s3.S3Payload;
import io.leoplatform.sdk.payload.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

@Singleton
public final class JSDKGzipWriter implements CompressionWriter {
    private static final Logger log = LoggerFactory.getLogger(JSDKGzipWriter.class);

    private static final String NEWLINE = "\n";
    private final S3JsonPayload streamJson;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public JSDKGzipWriter(S3JsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        this.streamJson = streamJson;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public FileSegment compressWithOffsets(Collection<EventPayload> payloads) {
        List<EntityPayload> entities = toEntityPayloads(payloads);
        String inflatedPayload = inflatedPayload(entities);
        byte[] compressedPayload = toGzip(inflatedPayload);

        String queue = getQueue(entities);
        Long start = 0L;
        Long records = (long) entities.size();
        Long end = records - 1;
        Long size = (long) inflatedPayload.getBytes(UTF_8).length;
        Long offset = 0L;
        Long gzipSize = (long) compressedPayload.length;
        Long gzipOffset = 0L;

        thresholdMonitor.addBytes(gzipSize);

        StorageEventOffset seo = new StorageEventOffset(queue, start, end, size, offset, records, gzipSize, gzipOffset);
        return new FileSegment(seo, compressedPayload);
    }

    @Override
    public ByteBuffer compress(S3Payload payload) {
        String inflatedPayload = streamJson.toJsonString(payload) + NEWLINE;
        byte[] compressedPayload = toGzip(inflatedPayload);

        thresholdMonitor.addBytes((long) compressedPayload.length);
        return ByteBuffer.wrap(compressedPayload);
    }

    private String inflatedPayload(List<EntityPayload> entities) {
        return entities.parallelStream()
                .map(streamJson::toJsonString)
                .collect(Collectors.joining(NEWLINE, "", NEWLINE));
    }

    private String getQueue(List<EntityPayload> entities) {
        return entities.stream()
                .map(EntityPayload::getEvent)
                .filter(Objects::nonNull)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No queue found in payload"));
    }

    private List<EntityPayload> toEntityPayloads(Collection<EventPayload> payloads) {
        return validate(payloads).stream()
                .map(streamJson::toEntity)
                .collect(toList());
    }

    private Collection<EventPayload> validate(Collection<EventPayload> payloads) {
        return Optional.ofNullable(payloads)
                .filter(p -> !p.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing payload"));
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
