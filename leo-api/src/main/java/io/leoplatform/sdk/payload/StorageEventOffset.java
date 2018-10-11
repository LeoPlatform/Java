package io.leoplatform.sdk.payload;

public class StorageEventOffset {
    private final String event;
    private final Long start;
    private final Long end;
    private final Long size;
    private final Long offset;
    private final Long records;
    private final Long gzipSize;
    private final Long gzipOffset;

    public StorageEventOffset(String event, Long start, Long end, Long size, Long offset, Long records,
                              Long gzipSize, Long gzipOffset) {
        this.event = event;
        this.start = start;
        this.end = end;
        this.size = size;
        this.offset = offset;
        this.records = records;
        this.gzipSize = gzipSize;
        this.gzipOffset = gzipOffset;
    }

    public String getEvent() {
        return event;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Long getSize() {
        return size;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getRecords() {
        return records;
    }

    public Long getGzipSize() {
        return gzipSize;
    }

    public Long getGzipOffset() {
        return gzipOffset;
    }
}
