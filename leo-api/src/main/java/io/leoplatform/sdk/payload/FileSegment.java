package io.leoplatform.sdk.payload;

public class FileSegment {
    private final StorageEventOffset offset;
    private final byte[] segment;

    public FileSegment(StorageEventOffset offset, byte[] segment) {
        this.offset = offset;
        this.segment = segment;
    }

    public StorageEventOffset getOffset() {
        return offset;
    }

    public byte[] getSegment() {
        return segment;
    }
}
