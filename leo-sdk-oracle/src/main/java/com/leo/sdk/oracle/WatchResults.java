package com.leo.sdk.oracle;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class WatchResults {
    private final Instant start;
    private final Instant end;
    private final List<String> tablesWatched;

    public WatchResults(Instant start, Instant end, List<String> tablesWatched) {
        this.start = Optional.ofNullable(start)
                .orElseThrow(() -> new IllegalArgumentException("Start time required"));
        this.end = Optional.ofNullable(end)
                .orElseThrow(() -> new IllegalArgumentException("End time required"));
        this.tablesWatched = Optional.ofNullable(tablesWatched)
                .orElse(Collections.emptyList());
    }

    public long listenSeconds() {
        return Duration.between(start, end).getSeconds();
    }

    public List<String> getTablesWatched() {
        return tablesWatched;
    }
}
