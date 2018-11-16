package io.leoplatform.sdk.changes;

import javax.json.JsonArray;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JsonDomainData {
    JsonArray toJson(ResultSet rs) throws SQLException;
}
