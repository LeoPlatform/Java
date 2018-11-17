package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;

import javax.json.Json;
import javax.json.JsonArray;
import java.util.Queue;

public class SimpleDomainResolver implements DomainResolver {
    @Override
    public JsonArray toResultJson(String sourceName, Queue<Field> fields) {
        //TODO: implement this correctly
        return Json.createArrayBuilder().build();
    }
}