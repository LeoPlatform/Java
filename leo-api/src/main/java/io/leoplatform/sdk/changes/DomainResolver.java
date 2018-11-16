package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;

import javax.json.JsonArray;
import java.util.List;

public interface DomainResolver {
    JsonArray toResultJson(String sourceName, List<Field> fields);
}
