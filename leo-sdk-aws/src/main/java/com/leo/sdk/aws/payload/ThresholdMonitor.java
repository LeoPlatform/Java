package com.leo.sdk.aws.payload;

import javax.inject.Singleton;

@Singleton
public interface ThresholdMonitor {
    void addBytes(long bytes);

    boolean isFailover();

    void end();
}
