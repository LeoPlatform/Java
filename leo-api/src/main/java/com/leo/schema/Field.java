package com.leo.schema;

import java.util.Objects;

public final class Field {
    private final String field;
    private final FieldType type;
    private final String value;

    public Field(String field, FieldType type, String value) {
        this.field = field;
        this.type = type;
        this.value = value;
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
        return Objects.equals(field, field1.field) &&
                type == field1.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, type, value);
    }
}
