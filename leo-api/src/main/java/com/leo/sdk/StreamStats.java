package com.leo.sdk;

import java.time.Duration;

public interface StreamStats {
    Long successes();

    Long failures();

    Duration totalTime();
}
