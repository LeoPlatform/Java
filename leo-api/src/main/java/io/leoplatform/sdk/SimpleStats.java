package io.leoplatform.sdk;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static java.time.Instant.EPOCH;

public final class SimpleStats implements StreamStats {
    private final Long successes;
    private final Long failures;
    private final Instant start;

    public SimpleStats(Long successes, Long failures, Instant start) {
        this.successes = valueOrZero(successes);
        this.failures = valueOrZero(failures);
        this.start = valueOrNow(start);
    }

    @Override
    public Long successes() {
        return successes;
    }

    @Override
    public Long failures() {
        return failures;
    }

    @Override
    public Duration totalTime() {
        return Duration.between(start, Instant.now());
    }

    private Long valueOrZero(Long val) {
        return Optional.ofNullable(val)
            .filter(v -> v >= 0)
            .orElse(0L);
    }

    private Instant valueOrNow(Instant val) {
        return Optional.ofNullable(val)
            .filter(v -> v.isAfter(EPOCH))
            .orElse(Instant.now());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleStats that = (SimpleStats) o;
        return successes.equals(that.successes) &&
            failures.equals(that.failures) &&
            start.equals(that.start);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successes, failures, start);
    }

    @Override
    public String toString() {
        return String.format("SimpleStats{successes=%d, failures=%d, start=%s}", successes, failures, start);
    }
}
