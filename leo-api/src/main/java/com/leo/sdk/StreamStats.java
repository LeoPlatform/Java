package com.leo.sdk;

import java.time.Duration;
import java.util.stream.Stream;

public interface StreamStats {
    Stream<String> successIds();

    Stream<String> failedIds();

    Duration totalTime();
}
