package com.leo.sdk.aws.kinesis;

public class KinesisPayload {
    private final byte[] records;
    private final String streamName;

    public KinesisPayload(byte[] records, String streamName) {
        this.records = records;
        this.streamName = streamName;
    }

    public byte[] getRecords() {
        return records;
    }

    public String getStreamName() {
        return streamName;
    }
}
