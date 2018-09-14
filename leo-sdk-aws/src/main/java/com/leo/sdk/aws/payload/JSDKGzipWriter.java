package com.leo.sdk.aws.payload;

import com.leo.sdk.PayloadIdentifier;
import com.leo.sdk.payload.*;
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
    private final StreamJsonPayload streamJson;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public JSDKGzipWriter(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        this.streamJson = streamJson;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public PayloadIdentifier compressWithNewlines(Collection<EventPayload> payloads) {
        List<EntityPayload> entities = toEntityPayloads(payloads);
        String inflatedPayload = inflatedPayload(entities);
        byte[] compressedPayload = toGzip(inflatedPayload);
        String plural = payloads.size() == 1 ? "" : "s";
        int inflatedLen = inflatedPayload.getBytes(UTF_8).length;
        int compressedLen = compressedPayload.length;
        log.info("Compression of {} payload{} from {} to {} bytes", payloads.size(), plural, inflatedLen, compressedLen);
        thresholdMonitor.addBytes(compressedLen);
        EntityPayload first = entities.iterator().next();
        return new PayloadIdentifier(first, ByteBuffer.wrap(compressedPayload));
    }

    private String inflatedPayload(List<EntityPayload> entities) {
        return entities.parallelStream()
                .map(streamJson::toJsonString)
                .collect(Collectors.joining(NEWLINE));
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

        StorageEventOffset seo = new StorageEventOffset(queue, start, end, size, offset, records, gzipSize, gzipOffset);
        return new FileSegment(seo, compressedPayload);
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
