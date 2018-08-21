package com.leo.schema;

import java.util.Objects;

public final class Field {
    private final String field;
    private final FieldType type;

    public Field(String field, FieldType type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public FieldType getType() {
        return type;
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
        return Objects.hash(field, type);
    }
}
