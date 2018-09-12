package com.leo.sdk.aws.payload;

public interface ThresholdMonitor {
    void addBytes(long bytes);

    boolean isFailover();

    void end();
}
