package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;
import io.leoplatform.schema.Field;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static io.leoplatform.schema.FieldType.INT;
import static io.leoplatform.schema.Op.UPDATE;
import static io.leoplatform.schema.Source.MYSQL;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public final class DomainObjectPayloadTest {

    @Mock
    private DomainResolver domainResolver;

    @Mock
    private PayloadWriter payloadWriter;

    @InjectMocks
    private DomainObjectPayload domainObjectPayload;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void mockTeardown() {
        Mockito.reset(domainResolver, payloadWriter);
    }

    @Test()
    public void testCollation() {
        int events = 6;
        when(domainResolver.toResultJson(anyString(), any())).thenReturn(genericArray(events));
        domainObjectPayload.loadChanges(events(events));
        verify(domainResolver, times(events)).toResultJson(anyString(), any());
    }

    @Test()
    public void testWriterException() {
        int events = 8;
        when(domainResolver.toResultJson(anyString(), any())).thenReturn(genericArray(events));
        doThrow(new RuntimeException()).when(payloadWriter).write(anyList());
        domainObjectPayload.loadChanges(events(events));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testEndException() {
        doThrow(new RuntimeException()).when(payloadWriter).end();
        domainObjectPayload.end();
    }

    private BlockingQueue<ChangeEvent> events(int count) {
        return IntStream.range(1, count + 1)
            .mapToObj(i -> event("TBL" + i, count))
            .collect(toCollection(LinkedBlockingQueue::new));
    }

    private ChangeEvent event(String name, int count) {
        return new ChangeEvent(MYSQL, UPDATE, name, fields(count * 20));
    }

    private List<Field> fields(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> field("Field" + i))
            .collect(toList());
    }

    private Field field(String name) {
        return new Field(name, INT, name + "_ID");
    }

    private JsonArray genericArray(int count) {
        return IntStream.range(0, count)
            .mapToObj(this::genericJsonField)
            .collect(Json::createArrayBuilder, JsonArrayBuilder::add, JsonArrayBuilder::addAll)
            .build();
    }

    private JsonObjectBuilder genericJsonField(int field) {
        return Json.createObjectBuilder()
            .add("FIELD1", "X" + field)
            .add("FIELD2", "Y" + field)
            .add("FIELD3", "Z" + field);

    }
}