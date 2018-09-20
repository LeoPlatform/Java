package com.leo.sdk.payload;

public interface ThresholdMonitor {
    void addBytes(Long bytes);

    boolean isFailover();

    void end();
}
