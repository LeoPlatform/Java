package com.leo.sdk.aws.payload;

public interface ThresholdMonitor {
    void addBytes(Long bytes);

    boolean isFailover();

    void end();
}
