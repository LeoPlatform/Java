package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;
import io.leoplatform.schema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

@Singleton
public class DomainObjectPayload implements ChangeReactor {
    private static final Logger log = LoggerFactory.getLogger(DomainObjectPayload.class);

    private final DomainResolver domainResolver;
    private final PayloadWriter payloadWriter;

    @Inject
    public DomainObjectPayload(DomainResolver domainResolver, PayloadWriter payloadWriter) {
        this.domainResolver = domainResolver;
        this.payloadWriter = payloadWriter;
    }

    @Override
    public void loadChanges(Queue<ChangeEvent> changeEvent) {
        List<JsonObject> domainObjects = distinctChanges(changeEvent).entrySet().stream()
            .map(this::toJson)
            .collect(toList());
        payloadWriter.write(domainObjects);
    }

    @Override
    public void end() {
        payloadWriter.end();
    }

    //TODO: make this work asynchronous
    private JsonObject toJson(Entry<String, Queue<Field>> changes) {
        log.info("Loading {} domain objects from {}", changes.getValue().size(), changes.getKey());
        JsonArray results = domainResolver.toResultJson(changes.getKey(), changes.getValue());
        return Json.createObjectBuilder()
            .add(changes.getKey(), results)
            .build();
    }

    private Map<String, Queue<Field>> distinctChanges(Queue<ChangeEvent> changeEvent) {

        return changeEvent.parallelStream()
            .filter(c -> Optional.of(c).map(ChangeEvent::getName).isPresent())
            .collect(toConcurrentMap(
                ChangeEvent::getName,
                c -> c.getFields().parallelStream()
                    .filter(f -> Optional.of(f).map(Field::getValue).isPresent())
                    .collect(toCollection(LinkedList::new)),
                (f1, f2) -> Stream.of(f1, f2)
                    .flatMap(Collection::stream)
                    .collect(toCollection(LinkedList::new)))
            );

    }
}
