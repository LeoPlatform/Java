package com.leo.sdk.aws.payload;

import com.jcraft.jzlib.GZIPOutputStream;
import com.leo.sdk.aws.s3.S3Payload;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.FileSegment;
import com.leo.sdk.payload.StorageEventOffset;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

@Singleton
public final class JCraftGzipWriter implements CompressionWriter {
    private static final Logger log = LoggerFactory.getLogger(JCraftGzipWriter.class);

    private static final String NEWLINE = "\n";
    private final S3JsonPayload streamJson;

    @Inject
    public JCraftGzipWriter(S3JsonPayload streamJson) {
        this.streamJson = streamJson;
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

    @Override
    public ByteBuffer compress(S3Payload payload) {
        String inflatedPayload = streamJson.toJsonString(payload) + NEWLINE;
        byte[] compressedPayload = toGzip(inflatedPayload);
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
        try (OutputStream os = new GZIPOutputStream(byteStream, 512, true)) {
            os.write(json.getBytes(UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Could not compress payload", e);
        }
    }
}
