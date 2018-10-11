package io.leoplatform.schema;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;

public final class SimpleChangeListener implements ChangeDestination {
    static final int DEFAULT_PORT = 47632;

    private final String host;
    private final Integer port;

    public SimpleChangeListener() {
        this(DEFAULT_PORT);
    }

    public SimpleChangeListener(String host) {
        this(host, DEFAULT_PORT);
    }

    public SimpleChangeListener(Integer port) {
        try {
            this.host = InetAddress.getLocalHost().getHostAddress();
            this.port = port;
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Unable to determine listener host name");
        }
    }

    public SimpleChangeListener(String host, Integer port) {
        this.host = host;
        this.port = Optional.ofNullable(port)
                .filter(p -> p <= 65535)
                .filter(p -> p >= 0)
                .orElseThrow(() -> new IllegalArgumentException("Port must be from 0-65535"));
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleChangeListener that = (SimpleChangeListener) o;
        return Objects.equals(host, that.host) &&
                Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return String.format("SimpleChangeListener{host='%s', port=%d}", host, port);
    }
}
