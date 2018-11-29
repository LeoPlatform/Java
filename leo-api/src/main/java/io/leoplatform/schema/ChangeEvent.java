package io.leoplatform.schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ChangeEvent {
    private final Source source;
    private final Op op;
    private final String name;
    private final List<Field> fields;

    public ChangeEvent(Source source, Op op, String name, List<Field> fields) {
        this.source = Optional.ofNullable(source)
            .orElseThrow(() -> new IllegalArgumentException("Change event source required"));
        this.op = Optional.ofNullable(op)
            .orElseThrow(() -> new IllegalArgumentException("Change event operation required"));
        this.name = Optional.ofNullable(name)
            .orElseThrow(() -> new IllegalArgumentException("Change event name required"));
        this.fields = Optional.ofNullable(fields)
            .orElseThrow(() -> new IllegalArgumentException("Change event fields required"));
    }

    public Source getSource() {
        return source;
    }

    public Op getOp() {
        return op;
    }

    public String getName() {
        return name;
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ChangeEvent that = (ChangeEvent) o;
        return source == that.source &&
            op == that.op &&
            name.equals(that.name) &&
            fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, op, name, fields);
    }

    @Override
    public String toString() {
        return String.format("ChangeEvent{source=%s, op=%s, name='%s', fields=%s}", source, op, name, fields);
    }
}
