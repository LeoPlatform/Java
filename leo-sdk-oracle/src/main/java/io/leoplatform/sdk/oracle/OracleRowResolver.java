package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.Field;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.changes.DomainQuery;
import io.leoplatform.sdk.changes.DomainResolver;
import io.leoplatform.sdk.changes.JsonDomainData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import static javax.json.JsonValue.EMPTY_JSON_ARRAY;

@Singleton
public class OracleRowResolver implements DomainResolver {
    private static final Logger log = LoggerFactory.getLogger(OracleRowResolver.class);
    private static final int BATCH_SIZE = 1_000;

    private final DomainQuery domainQuery;
    private final JsonDomainData jsonDomainData;
    private final ExecutorManager manager;

    @Inject
    public OracleRowResolver(DomainQuery domainQuery, JsonDomainData jsonDomainData, ExecutorManager manager) {
        this.domainQuery = domainQuery;
        this.jsonDomainData = jsonDomainData;
        this.manager = manager;
    }

    @Override
    public JsonArray toResultJson(String sourceName, BlockingQueue<Field> fields) {
        return drainAsBatches(fields)
            .parallel()
            .map(batch -> generateSql(sourceName, batch))
            .map(this::toJsonAsync)
            .map(CompletableFuture::join)
            .flatMap(Collection::stream)
            .collect(Json::createArrayBuilder, JsonArrayBuilder::add, JsonArrayBuilder::addAll)
            .build();
    }

    private String generateSql(String sourceName, List<Field> batch) {
        return domainQuery.generateSql(sourceName, batch);
    }

    private CompletableFuture<JsonArray> toJsonAsync(String sql) {
        return CompletableFuture
            .supplyAsync(() -> jsonDomainData.toJson(sql), manager.get())
            .exceptionally(th -> EMPTY_JSON_ARRAY);
    }

    private Stream<List<Field>> drainAsBatches(BlockingQueue<Field> fields) {
        Builder<List<Field>> batchBuilder = Stream.builder();
        while (!fields.isEmpty()) {
            List<Field> batch = new LinkedList<>();
            fields.drainTo(batch, BATCH_SIZE);
            batchBuilder.accept(batch);
        }
        return batchBuilder.build();
    }
}
