package com.leo.schema;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

public class SimpleChangeListener implements ChangeListener {
    private static final int DEFAULT_PORT = 47632;

    private final String host;
    private final Integer port;

    public SimpleChangeListener() {
        this(DEFAULT_PORT);
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
                .orElse(DEFAULT_PORT);
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public Integer getPort() {
        return port;
    }
}
