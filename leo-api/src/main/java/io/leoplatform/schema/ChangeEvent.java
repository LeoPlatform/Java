package io.leoplatform.schema;

import java.util.List;
import java.util.Objects;

public final class ChangeEvent {
    private final Source source;
    private final Op op;
    private final String name;
    private final List<Field> fields;

    public ChangeEvent(Source source, Op op, String name, List<Field> fields) {
        this.source = source;
        this.op = op;
        this.name = name;
        this.fields = fields;
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
                Objects.equals(name, that.name) &&
                Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, op, name, fields);
    }
}
