package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.ChangeSource;
import io.leoplatform.schema.Field;
import io.leoplatform.sdk.changes.DomainResolver;
import io.leoplatform.sdk.changes.JsonDomainData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.leoplatform.schema.FieldType.STRING;
import static java.util.stream.Collectors.toCollection;

public class OracleRowResolver implements DomainResolver {
    private static final Logger log = LoggerFactory.getLogger(OracleRowResolver.class);

    private final ChangeSource source;
    private final JsonDomainData jsonDomainData;

    @Inject
    public OracleRowResolver(ChangeSource source, JsonDomainData jsonDomainData) {
        this.source = source;
        this.jsonDomainData = jsonDomainData;
    }

    @Override
    public JsonArray toResultJson(String sourceName, List<Field> fields) {
        List<String> changedRows = rowIds(fields);
        JsonArrayBuilder builder = Json.createArrayBuilder();
        while (!changedRows.isEmpty()) {
            int toElement = Math.min(changedRows.size(), 1_000);
            String rowIds = IntStream.range(0, toElement)
                .mapToObj(changedRows::remove)
                .collect(Collectors.joining("','", "'", "'"));
            builder.add(toJson(sourceName, rowIds));
        }
        return builder.build();
    }

    private JsonArray toJson(String sourceName, String rows) {
        try (Connection conn = source.connection()) {
            String sql = String.format("SELECT * FROM %s WHERE ROWID IN (%s)", sourceName, rows);
            try (Statement stmt = conn.createStatement()) {
                return jsonDomainData.toJson(stmt.executeQuery(sql));
            }
        } catch (SQLException s) {
            log.warn("Unable to contact Oracle database");
            throw new IllegalStateException("Error retrieving source changes", s);
        }
    }

    private List<String> rowIds(List<Field> fields) {
        return fields.parallelStream()
            .filter(f -> f.getType() == STRING)
            .filter(f -> f.getField().equals("ROWID"))
            .map(Field::getValue)
            .collect(toCollection(LinkedList::new));
    }
}
