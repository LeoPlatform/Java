package io.leoplatform.sdk.oracle;

import com.typesafe.config.ConfigFactory;
import io.leoplatform.schema.Field;
import io.leoplatform.sdk.changes.DomainQuery;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.stream.Stream;

import static io.leoplatform.schema.FieldType.STRING;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class OracleDomainQueryTest {

    private DomainQuery oracleQuery = new OracleDomainQuery(ConfigFactory.load("oracle_config.properties"));

    @Test
    public void testDefaultSql() {
        String actual = oracleQuery.generateSql("TBL", Collections.singletonList(simpleField()));
        String expected = "SELECT * FROM TBL WHERE ROWID IN ('ABC')";
        assertEquals(actual, expected, "Default SQL mismatch");
    }

    @Test
    public void testMultiDefaultSql() {
        String actual = oracleQuery.generateSql("TBL", Stream.of(simpleField(), simpleField2()).collect(toList()));
        String expected = "SELECT * FROM TBL WHERE ROWID IN ('ABC','DEF')";
        assertEquals(actual, expected, "Default multi SQL mismatch");
    }

    @Test
    public void testCustomSql() {
        String actual = oracleQuery.generateSql("TBL_CUST_1", Collections.singletonList(simpleField()));
        String expected = "SELECT A, B, C FROM TBL_CUST_1 WHERE ROWID IN ('ABC')";
        assertEquals(actual, expected, "Custom SQL mismatch");
    }

    @Test
    public void testMultiCustomSql() {
        String actual = oracleQuery.generateSql("TBL_CUST_1", Stream.of(simpleField(), simpleField2()).collect(toList()));
        String expected = "SELECT A, B, C FROM TBL_CUST_1 WHERE ROWID IN ('ABC','DEF')";
        assertEquals(actual, expected, "Custom multi SQL mismatch");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testMissingTable() {
        oracleQuery.generateSql("", Collections.singletonList(simpleField()));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testMissingField() {
        oracleQuery.generateSql("TBL", Collections.emptyList());
    }

    private Field simpleField() {
        return new Field("ROWID", STRING, "ABC");
    }

    private Field simpleField2() {
        return new Field("ROWID", STRING, "DEF");
    }
}