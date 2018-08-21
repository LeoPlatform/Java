package com.leo.sdk.oracle;

import com.leo.schema.SimpleChangeListener;

import java.util.Properties;

public final class OracleListeners {
    public static OracleChangeListener of(String host, Integer port) {
        return OracleListeners.of(host, port, new Properties());
    }

    public static OracleChangeListener of(String host, Integer port, Properties props) {
        return new SimpleOracleChangeListener(new SimpleChangeListener(host, port), props);
    }
}
