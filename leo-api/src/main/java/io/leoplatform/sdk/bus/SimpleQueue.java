package io.leoplatform.sdk.bus;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class SimpleQueue implements StreamQueue {
    private final String name;
    private final List<String> tags;

    public SimpleQueue(String name) {
        this(name, Collections.emptyList());
    }

    public SimpleQueue(String name, List<String> tags) {
        this.name = Optional.ofNullable(name)
                .map(String::trim)
                .filter(n -> !n.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing queue name"));
        this.tags = Optional.ofNullable(tags)
                .orElse(Collections.emptyList());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<String> tags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleQueue that = (SimpleQueue) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return String.format("SimpleQueue{name='%s', tags='%s'}", name, String.join(",", tags));
    }
}
