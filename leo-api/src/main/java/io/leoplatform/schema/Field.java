package io.leoplatform.schema;

import java.util.Objects;
import java.util.Optional;

public final class Field {
    private final String field;
    private final FieldType type;
    private final String value;

    public Field(String field, FieldType type, String value) {
        this.field = Optional.ofNullable(field)
            .orElseThrow(() -> new IllegalArgumentException("Field name required"));
        this.type = Optional.ofNullable(type)
            .orElseThrow(() -> new IllegalArgumentException("Field type required"));
        this.value = Optional.ofNullable(value)
            .orElseThrow(() -> new IllegalArgumentException("Field value required"));
    }

    public String getField() {
        return field;
    }

    public FieldType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Field field1 = (Field) o;
        return field.equals(field1.field) &&
            type == field1.type &&
            value.equals(field1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, type, value);
    }

    @Override
    public String toString() {
        return String.format("Field{field='%s', type=%s, value='%s'}", field, type, value);
    }
}
