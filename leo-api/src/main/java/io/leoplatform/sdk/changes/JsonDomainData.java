package io.leoplatform.sdk.changes;

import javax.json.JsonArray;

public interface JsonDomainData {
    JsonArray toJson(String query);
}
