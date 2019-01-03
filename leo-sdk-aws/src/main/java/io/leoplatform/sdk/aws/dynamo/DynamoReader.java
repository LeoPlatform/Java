package io.leoplatform.sdk.aws.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import io.leoplatform.sdk.aws.AWSResources;
import io.leoplatform.sdk.bus.OffloadingBot;
import io.leoplatform.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.TOTAL;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;
import static java.time.ZoneOffset.UTC;
import static javax.json.JsonValue.EMPTY_JSON_OBJECT;

@Singleton
public final class DynamoReader {
    private static final Logger log = LoggerFactory.getLogger(DynamoReader.class);

    private final ObjectMapper mapper = buildMapper();
    private static final DateTimeFormatter checkpointFormat = DateTimeFormatter
        .ofPattern("'z/'uuuu'/'MM'/'dd")
        .withZone(UTC);
    private final String cronTable;
    private final String eventTable;
    private final String streamTable;
    private final DynamoDB dynamoDB;

    @Inject
    public DynamoReader(AWSResources credentials) {
        this.cronTable = credentials.cronTable();
        this.eventTable = credentials.eventTable();
        this.streamTable = credentials.streamTable();
        this.dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentials.credentials())
            .withRegion(credentials.region())
            .withClientConfiguration(clientConfig())
            .build());
    }

    public Stream<EntityPayload> events(OffloadingBot bot) {

        Map<String, List<Item>> eventItems = eventItems(bot);
        JsonObject queueMetadata = queueMetadata(eventItems);
        String checkpoint = findCheckpoint(bot, eventItems, queueMetadata);
        log.info("Reading event from {}", checkpoint);

        QuerySpec eventQuery = new QuerySpec()
            .withKeyConditionExpression("#event = :event and #key between :start and :maxkey")
            .withNameMap(new LinkedHashMap<String, String>() {{
                put("#event", "event");
                put("#key", "end");
            }})
            .withValueMap(new LinkedHashMap<String, Object>() {{
                put(":event", bot.source().name());
                put(":maxkey", maxEid(queueMetadata));
                put(":start", checkpoint);
            }})
            .withMaxResultSize(50)
            .withReturnConsumedCapacity(TOTAL);
        log.debug("Executing query", eventQuery.getSelect());

        return StreamSupport.stream(dynamoDB.getTable(streamTable).query(eventQuery).spliterator(), true)
            .map(Item::toJSON)
            .map(this::toEntityPayload);
    }

    private String maxEid(JsonObject queueMetadata) {
        return Optional.of(queueMetadata)
            .map(m -> m.getString("max_eid"))
            .orElse("");
    }

    private String findCheckpoint(OffloadingBot bot, Map<String, List<Item>> eventItems, JsonObject queueMetadata) {
        int version = findVersion(queueMetadata);
        if (version >= 2) {
            return checkpoint(bot.name(), eventItems);
        } else {
            return checkpointLegacy(eventItems);
        }
    }

    private JsonObject queueMetadata(Map<String, List<Item>> eventItems) {
        return eventItems.getOrDefault(eventTable, Collections.emptyList()).stream()
            .findFirst()
            .map(Item::toJSON)
            .map(this::toJson)
            .orElse(EMPTY_JSON_OBJECT);
    }

    private Map<String, List<Item>> eventItems(OffloadingBot bot) {
        TableKeysAndAttributes cron = new TableKeysAndAttributes(cronTable)
            .addHashOnlyPrimaryKey("id", bot.name());
        TableKeysAndAttributes event = new TableKeysAndAttributes(eventTable)
            .addHashOnlyPrimaryKey("event", bot.source().name());

        return dynamoDB.batchGetItem(cron, event)
            .getTableItems();
    }

    private int findVersion(JsonObject queueMetadata) {
        return Optional.of(queueMetadata)
            .map(q -> q.getJsonNumber("v"))
            .map(JsonNumber::intValue)
            .orElse(0);
    }

    private String checkpointLegacy(Map<String, List<Item>> eventItems) {
        return Optional.of(botJson(eventItems))
            .map(b -> b.getString("checkpoint"))
            .orElse(defaultCheckpointStart());
    }

    private String checkpoint(String botName, Map<String, List<Item>> eventItems) {
        return Optional.of(botJson(eventItems))
            .map(b -> b.getJsonObject("checkpoints"))
            .map(c -> c.getJsonObject("read"))
            .map(c -> c.getJsonObject("queue:" + botName))
            .map(c -> c.getString("checkpoint"))
            .orElse(defaultCheckpointStart());

    }

    private JsonObject botJson(Map<String, List<Item>> eventItems) {
        return eventItems.getOrDefault(cronTable, Collections.emptyList()).stream()
            .findFirst()
            .map(Item::toJSON)
            .map(this::toJson)
            .orElse(EMPTY_JSON_OBJECT);
    }

    private JsonObject toJson(String raw) {
        try {
            return Json.createReader(new StringReader(raw)).readObject();
        } catch (Exception e) {
            log.warn("Unable to read DynamoDB JSON response", e);
            return EMPTY_JSON_OBJECT;
        }
    }

    private String defaultCheckpointStart() {
        return checkpointFormat.format(Instant.now());
    }

    public void end() {
        dynamoDB.shutdown();
    }

    private EntityPayload toEntityPayload(String json) {
        try {
            return mapper.readValue(json, EntityPayload.class);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid entity payload JSON", e);
        }
    }

    private ClientConfiguration clientConfig() {
        return PredefinedClientConfigurations.dynamoDefault()
            .withConnectionTimeout(2000)
            .withRequestTimeout(5000)
            .withMaxErrorRetry(2);
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
            .setSerializationInclusion(ALWAYS)
            .registerModule(new JSR353Module());
    }

}
