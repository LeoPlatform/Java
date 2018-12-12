package io.leoplatform.sdk.aws.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import io.leoplatform.sdk.aws.AWSResources;
import io.leoplatform.sdk.bus.OffloadingBot;
import io.leoplatform.sdk.payload.EntityPayload;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

@Singleton
public final class DynamoReader {

    private final ObjectMapper mapper = buildMapper();
    private final String cronTable;
    private final String eventTable;
    private final DynamoDB dynamoDB;

    @Inject
    public DynamoReader(AWSResources credentials) {
        this.cronTable = credentials.cronTable();
        this.eventTable = credentials.eventTable();
        this.dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentials.credentials())
            .withRegion(credentials.region())
            .withClientConfiguration(clientConfig())
            .build());
    }

    public Stream<EntityPayload> events(OffloadingBot bot) {

        TableKeysAndAttributes cron = new TableKeysAndAttributes(cronTable)
            .addHashOnlyPrimaryKey("id", bot.name());
//        TableKeysAndAttributes event = new TableKeysAndAttributes(eventTable)
//                .addHashOnlyPrimaryKey("event", bot.source().name());

//        BatchGetItemOutcome outcome = dynamoDB.batchGetItem(cron, event);
        BatchGetItemOutcome outcome = dynamoDB.batchGetItem(cron);

        return outcome.getTableItems().values().stream()
            .flatMap(Collection::stream)
            .map(Item::toJSON)
            .map(this::toEntityPayload);

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
