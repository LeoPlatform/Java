package io.leoplatform.sdk.aws.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import io.leoplatform.sdk.bus.OffloadingBot;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.payload.EntityPayload;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

@Singleton
public final class DynamoReader {

    private final ObjectMapper mapper = buildMapper();
    private final String cronTable;
    private final String eventTable;
    private final DynamoDB dynamoDB;

    @Inject
    public DynamoReader(ConnectorConfig config) {
        this.cronTable = config.value("Cron");
        this.eventTable = config.value("Event");
        this.dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentials(config))
            .withRegion(config.valueOrElse("Region", "us-east-1"))
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

    private AWSCredentialsProvider credentials(ConnectorConfig config) {
        try {
            return Optional.of(config.valueOrElse("AwsProfile", ""))
                .map(String::trim)
                .filter(profile -> !profile.isEmpty())
                .map(ProfileCredentialsProvider::new)
                .filter(p -> p.getCredentials() != null)
                .map(AWSCredentialsProvider.class::cast)
                .orElse(DefaultAWSCredentialsProviderChain.getInstance());
        } catch (Exception e) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
            .setSerializationInclusion(ALWAYS)
            .registerModule(new JSR353Module());
    }

}
