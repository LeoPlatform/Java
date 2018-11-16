package io.leoplatform.sdk.changes;

import javax.json.JsonObject;
import java.util.List;

public interface PayloadWriter {
    void write(List<JsonObject> domainObjects);

    void end();
}
