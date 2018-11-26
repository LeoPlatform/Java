package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.ChangeSource;
import io.leoplatform.schema.Field;
import io.leoplatform.sdk.changes.DomainQuery;
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
import java.util.concurrent.BlockingQueue;

public class OracleRowResolver implements DomainResolver {
    private static final Logger log = LoggerFactory.getLogger(OracleRowResolver.class);

    private final ChangeSource source;
    private final DomainQuery domainQuery;
    private final JsonDomainData jsonDomainData;

    @Inject
    public OracleRowResolver(ChangeSource source, DomainQuery domainQuery, JsonDomainData jsonDomainData) {
        this.source = source;
        this.domainQuery = domainQuery;
        this.jsonDomainData = jsonDomainData;
    }

    @Override
    public JsonArray toResultJson(String sourceName, BlockingQueue<Field> fields) {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        while (!fields.isEmpty()) {
            List<Field> values = new LinkedList<>();
            fields.drainTo(values, 1_000);
            String sql = domainQuery.generateSql(sourceName, values);
            builder.add(toJson(sql));
        }
        return builder.build();
    }

    private JsonArray toJson(String sql) {
        try (Connection conn = source.connection()) {
            try (Statement stmt = conn.createStatement()) {
                return jsonDomainData.toJson(stmt.executeQuery(sql));
            } catch (SQLException se) {
                log.error("Invalid SQL discovered {}", sql);
                throw new IllegalArgumentException(se);
            }
        } catch (SQLException s) {
            log.warn("Unable to contact Oracle database");
            throw new IllegalStateException("Error retrieving source changes", s);
        }
    }
}
