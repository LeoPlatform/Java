package com.leo.sdk.oracle;

import com.leo.schema.SimpleChangeListener;

import java.util.Properties;

public final class OracleListeners {
    public static OracleChangeDestination of() {
        return new SimpleOracleChangeDestination();
    }

    public static OracleChangeDestination of(String host) {
        return new SimpleOracleChangeDestination(new SimpleChangeListener(host));
    }

    public static OracleChangeDestination of(Integer port) {
        return new SimpleOracleChangeDestination(new SimpleChangeListener(port));
    }

    public static OracleChangeDestination of(String host, Integer port) {
        return OracleListeners.of(host, port, new Properties());
    }

    public static OracleChangeDestination of(String host, Integer port, Properties props) {
        return new SimpleOracleChangeDestination(new SimpleChangeListener(host, port), props);
    }
}
