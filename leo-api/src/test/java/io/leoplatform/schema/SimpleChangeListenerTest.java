package io.leoplatform.schema;

import io.leoplatform.sdk.changes.SimpleChangeListener;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import static io.leoplatform.sdk.changes.SimpleChangeListener.DEFAULT_PORT;
import static org.testng.Assert.*;

public final class SimpleChangeListenerTest {

    @Test
    public void testGetDefaultHost() throws UnknownHostException {
        ChangeDestination actual = new SimpleChangeListener();
        assertEquals(actual.getHost(), InetAddress.getLocalHost().getHostAddress(), "Invalid default host");
    }

    @Test
    public void testGetDefaultPort() {
        ChangeDestination actual = new SimpleChangeListener();
        assertEquals(actual.getPort(), Integer.valueOf(DEFAULT_PORT), "Invalid default host");
    }

    @Test
    public void testGetHost() {
        String host = "abcdefg";
        ChangeDestination actual = new SimpleChangeListener(host);
        assertEquals(actual.getHost(), host, "Invalid host");
    }

    @Test
    public void testGetPort() {
        Integer port = 12345;
        ChangeDestination actual = new SimpleChangeListener(port);
        assertEquals(actual.getPort(), port, "Invalid port");
    }

    @Test
    public void testGetHostAndPort() {
        String host = "abcdefgh";
        Integer port = 65123;
        ChangeDestination actual = new SimpleChangeListener(host, port);
        boolean matchingHost = actual.getHost().equals(host);
        boolean matchingPort = actual.getPort().equals(port);
        assertTrue(matchingHost && matchingPort, "Invalid host port combination");
    }

    @Test
    public void testEquals() {
        ChangeDestination cl1 = new SimpleChangeListener("abcdefghij", 65012);
        ChangeDestination cl2 = new SimpleChangeListener("abcdefghij", 65012);
        assertEquals(cl1, cl2, "Not equal values");
    }

    @Test
    public void testSame() {
        ChangeDestination cl1 = new SimpleChangeListener("abcdefghij", 65012);
        assertEquals(cl1, cl1, "Not same instance");
    }

    @Test
    public void testDifferent() {
        final String host = "abcdefghij";
        final int port = 65012;
        ChangeDestination cl1 = new ChangeDestination() {
            @Override
            public String getHost() {
                return host;
            }

            @Override
            public Integer getPort() {
                return port;
            }
        };
        ChangeDestination cl2 = new SimpleChangeListener(host, port);
        assertNotEquals(cl1, cl2, "Not different instance");
    }

    @Test
    public void testUnequal() {
        ChangeDestination cl1 = new SimpleChangeListener("abcdefghij", 65012);
        ChangeDestination cl2 = new SimpleChangeListener("abcdefghij", 65011);
        assertNotEquals(cl1, cl2, "Not unequal");
    }

    @Test
    public void testNull() {
        ChangeDestination cl1 = new SimpleChangeListener("abcdefghij", 65012);
        assertNotEquals(cl1, null, "Should not be equal to Null");
    }

    @Test
    public void testHashCode() {
        Set<ChangeDestination> cl = new HashSet<ChangeDestination>() {{
            add(new SimpleChangeListener("abcdefghij", 65012));
            add(new SimpleChangeListener("abcdefghij", 65012));
        }};
        assertEquals(cl.size(), 1, "Invalid hashcode");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testPortHigh() {
        new SimpleChangeListener("abcdefghij", 65536);
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testPortLow() {
        new SimpleChangeListener("abcdefghij", -1);
    }
}