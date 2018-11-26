package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;

import java.util.List;

public interface DomainQuery {
    String generateSql(String source, List<Field> values);
}
