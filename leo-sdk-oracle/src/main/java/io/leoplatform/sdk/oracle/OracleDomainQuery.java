package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.leoplatform.schema.Field;
import io.leoplatform.sdk.changes.DomainQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static io.leoplatform.schema.FieldType.STRING;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class OracleDomainQuery implements DomainQuery {
    private static final Logger log = LoggerFactory.getLogger(OracleDomainQuery.class);
    private static final Config cfg = ConfigFactory.load("oracle_config.properties");

    @Override
    public String generateSql(String source, List<Field> values) {
        if (values.size() > 1_000) {
            throw new IllegalArgumentException("Too many values for Oracle IN clause");
        }
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
        return values.parallelStream()
            .filter(f -> f.getType() == STRING)
            .map(Field::getValue)
            .collect(toList());
    }

    private Optional<String> customSql(String source, String id, List<String> values) {
        return valueForTable(source)
            .map(s -> s.replace("{TABLE}", source))
            .map(s -> s.replace("{ID}", id))
            .map(s -> s.replace("{VALUES}", valuesClause(values)));
    }

    private Optional<String> valueForTable(String source) {
        try {
            return Optional.of(cfg.getString("oracle." + source));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private String defaultSql(String source, String id, List<String> values) {
        String valuesAsString = valuesClause(values);
        String src = Optional.ofNullable(source)
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
