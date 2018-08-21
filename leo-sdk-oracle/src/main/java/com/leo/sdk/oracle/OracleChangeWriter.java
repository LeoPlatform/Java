package com.leo.sdk.oracle;

import com.leo.schema.*;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.payload.SimplePayload;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.TableChangeDescription;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static oracle.jdbc.dcn.TableChangeDescription.TableOperation;

public final class OracleChangeWriter implements DatabaseChangeListener {
    private static final Map<TableOperation, Op> supportedOps = supportedOps();
    private final PlatformStream stream;

    public OracleChangeWriter(PlatformStream stream) {
        this.stream = stream;
    }

    @Override
    public void onDatabaseChangeNotification(DatabaseChangeEvent changeEvent) {
        validateChangeEvents(changeEvent)
                .parallelStream()
                .flatMap(this::toEvents)
                .map(this::toJson)
                .map(this::toPayload)
                .forEach(stream::load);
    }

    public CompletableFuture<StreamStats> end() {
        return stream.end();
    }

    private SimplePayload toPayload(JsonObject jsonObject) {
        return () -> jsonObject;
    }

    private JsonObject toJson(ChangeEvent changeEvent) {
        JsonArrayBuilder fields = changeEvent.getFields().stream()
                .collect(Json::createArrayBuilder,
                        (b, f) -> b.add(Json.createObjectBuilder()
                                .add("field", "bla")
                                .add("type", "blah")),
                        JsonArrayBuilder::addAll);
        return Json.createObjectBuilder()
                .add("source", changeEvent.getSource().name())
                .add("op", changeEvent.getOp().name())
                .add("name", changeEvent.getName())
                .add("fields", fields)
                .build();
    }

    private List<TableChangeDescription> validateChangeEvents(DatabaseChangeEvent changeEvent) {
        return Optional.ofNullable(changeEvent)
                .map(DatabaseChangeEvent::getTableChangeDescription)
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }

    private Stream<ChangeEvent> toEvents(TableChangeDescription desc) {
        String table = tableName(desc);

        return Optional.ofNullable(desc)
                .filter(d -> !table.isEmpty())
                .map(TableChangeDescription::getTableOperations)
                .orElse(EnumSet.noneOf(TableOperation.class))
                .stream()
                .filter(supportedOps::containsKey)
                .map(supportedOps::get)
                .map(o -> toEvent(o, table));
    }

    private ChangeEvent toEvent(Op op, String tableName) {
        List<Field> fields = Collections.singletonList(new Field("ROWID", FieldType.STRING));
        return new ChangeEvent(Source.ORACLE, op, tableName, fields);
    }

    private String tableName(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
                .map(TableChangeDescription::getTableName)
                .orElse("");
    }

    private static Map<TableOperation, Op> supportedOps() {
        return new EnumMap<TableOperation, Op>(TableOperation.class) {{
            put(TableOperation.INSERT, Op.INSERT);
            put(TableOperation.UPDATE, Op.UPDATE);
            put(TableOperation.DELETE, Op.DELETE);
        }};
    }
}
