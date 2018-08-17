package com.leo.sdk.oracle;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class WatchResults {
    private final Instant start;
    private final Instant end;
    private final List<String> tablesWatched;

    public WatchResults(Instant start, Instant end, List<String> tablesWatched) {
        this.start = start;
        this.end = end;
        this.tablesWatched = tablesWatched;
    }

    public long listenSeconds() {
        return Duration.between(start, end).getSeconds();
    }

    public List<String> getTablesWatched() {
        return tablesWatched;
    }
}
