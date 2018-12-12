package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import io.leoplatform.schema.Field;
import io.leoplatform.sdk.changes.DomainQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

import static io.leoplatform.schema.FieldType.STRING;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Singleton
public final class OracleDomainQuery implements DomainQuery {
    private static final Logger log = LoggerFactory.getLogger(OracleDomainQuery.class);

    private final Config oracleConfig;

    @Inject
    public OracleDomainQuery(Config oracleConfig) {
        this.oracleConfig = oracleConfig;
    }

    @Override
    public String generateSql(String source, List<Field> values) {
        String id = tableId(values);
        List<String> queryValues = queryValues(values);

        return customSql(source, id, queryValues)
            .orElse(defaultSql(source, id, queryValues));
    }

    private String tableId(List<Field> values) {
        return values.parallelStream()
            .map(Field::getField)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Missing field ID"));
    }

    private List<String> queryValues(List<Field> values) {
        if (values.size() > 1_000) {
            throw new IllegalArgumentException("Too many values for Oracle IN clause");
        }
        return values.parallelStream()
            .filter(f -> f.getType() == STRING)
            .map(Field::getValue)
            .collect(toList());
    }

    private Optional<String> customSql(String source, String id, List<String> values) {
        Optional<String> customSql = valueForTable(source)
            .map(s -> s.replace("{TABLE}", source))
            .map(s -> s.replace("{ID}", id))
            .map(s -> s.replace("{VALUES}", valuesClause(values)));
        customSql.ifPresent(s -> log.debug("Custom domain query: {}", s));
        return customSql;
    }

    private Optional<String> valueForTable(String source) {
        try {
            return Optional.of(oracleConfig.getString("oracle." + source));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private String defaultSql(String source, String id, List<String> values) {
        String valuesAsString = valuesClause(values);
        Optional.ofNullable(source)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Missing table name"));
        return String.format("SELECT * FROM %s WHERE %s IN (%s)", source, id, valuesAsString);
    }

    private String valuesClause(List<String> values) {
        return values.stream()
            .collect(joining("','", "'", "'"));
    }
}
