package io.leoplatform.sdk.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

@Singleton
public class JacksonDomainJson implements JsonDomainData {
    private static final Logger log = LoggerFactory.getLogger(JacksonDomainJson.class);

    @Inject
    public JacksonDomainJson() {
    }

    @Override
    public JsonArray toJson(ResultSet rs) throws SQLException {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        ResultSetMetaData metaData = rs.getMetaData();
        int numCols = metaData.getColumnCount();
        Map<Integer, String> colNames = colNames(metaData, numCols);

        while (rs.next()) {
            JsonObjectBuilder b = Json.createObjectBuilder();
            IntStream.rangeClosed(1, numCols)
                .forEachOrdered(colNum -> {
                    String colName = colNames.get(colNum);
                    String colValue = colValue(rs, colNum);
                    b.add(colName, colValue);
                });
            arrayBuilder.add(b.build());
        }
        return arrayBuilder.build();
    }

    private String colValue(ResultSet rs, int colNum) {
        try {
            return rs.getObject(colNum).toString();
        } catch (SQLException s) {
            log.warn("Unable to retrieve value for column {}", colNum);
            return "";
        }
    }

    private Map<Integer, String> colNames(ResultSetMetaData metaData, int numCols) {
        return IntStream.rangeClosed(1, numCols)
            .mapToObj(colNum -> columnLabelEntry(metaData, colNum))
            .collect(toMap(Entry::getKey, Entry::getValue, (s1, s2) -> s1));
    }

    private SimpleImmutableEntry<Integer, String> columnLabelEntry(ResultSetMetaData metaData, int colNum) {
        String colLabel = Optional.of(metaData)
            .map(m -> colLabel(colNum, m))
            .map(String::trim)
            .filter(c -> !c.isEmpty())
            .orElse("COL" + colNum);
        return new SimpleImmutableEntry<>(colNum, colLabel);
    }

    private String colLabel(int colNum, ResultSetMetaData m) {
        try {
            return m.getColumnLabel(colNum);
        } catch (SQLException e) {
            return "COL" + colNum;
        }
    }
}
