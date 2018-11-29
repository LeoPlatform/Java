package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.Field;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.changes.DomainQuery;
import io.leoplatform.sdk.changes.JsonDomainData;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static io.leoplatform.schema.FieldType.STRING;
import static java.util.stream.Collectors.toCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

public class OracleRowResolverTest {

    @Mock
    private DomainQuery domainQuery;

    @Mock
    private JsonDomainData jsonDomainData;

    @Mock
    private ExecutorManager manager;

    @InjectMocks
    private OracleRowResolver oracleRowResolver;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @BeforeMethod
    public void mockSetup() {
        when(manager.get()).thenReturn(Runnable::run);
        when(domainQuery.generateSql(anyString(), anyList())).thenReturn(genericSql());
    }

    @AfterMethod
    public void mockTeardown() {
        Mockito.reset(domainQuery, jsonDomainData, manager);
    }

    @Test
    public void testSmallQueueSize() {
        int fields = 3;
        when(jsonDomainData.toJson(anyString())).thenReturn(genericArray(fields));
        JsonArray a = oracleRowResolver.toResultJson("TBL1", fields(fields));
        assertEquals(a.size(), fields, "Incorrect small queue field count");
    }

    @Test
    public void testSmallQueueBatch() {
        int fields = 4;
        when(jsonDomainData.toJson(anyString())).thenReturn(genericArray(fields));
        oracleRowResolver.toResultJson("TBL1", fields(fields));
        verify(jsonDomainData, times(1)).toJson(anyString());
    }

    @Test
    public void testLargeQueueSize() {
        int fields = 3_500;
        when(jsonDomainData.toJson(anyString())).thenReturn(genericArray(fields));
        JsonArray a = oracleRowResolver.toResultJson("TBL1", fields(fields));
        assertEquals(a.size(), fields * 4, "Incorrect large queue field count");
    }

    @Test
    public void testLargeQueueBatch() {
        int fields = 3_600;
        when(jsonDomainData.toJson(anyString())).thenReturn(genericArray(fields));
        oracleRowResolver.toResultJson("TBL1", fields(fields));
        verify(jsonDomainData, times(4)).toJson(anyString());
    }

    @Test
    public void testDomainException() {
        when(jsonDomainData.toJson(anyString())).thenThrow(IllegalStateException.class);
        JsonArray a = oracleRowResolver.toResultJson("TBL1", fields(3));
        assertEquals(a, Json.createArrayBuilder().build(), "Domain data exception incorrectly handled");
    }

    private BlockingQueue<Field> fields(int count) {
        return IntStream.range(1, count + 1)
            .mapToObj(i -> oraField("Field" + i))
            .collect(toCollection(LinkedBlockingQueue::new));
    }

    private String genericSql() {
        return "SELECT * FROM TBL";
    }

    private Field oraField(String name) {
        return new Field(name, STRING, "ROWID");
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
