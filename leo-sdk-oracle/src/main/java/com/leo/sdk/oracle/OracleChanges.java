package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import oracle.jdbc.OracleConnection;

import java.util.Collection;
import java.util.Optional;

public final class OracleChanges {
    public static OracleChangeLoader of(PlatformStream stream, OracleChangeSource source) {
        return DaggerOraclePlatform.builder()
                .platformStream(stream)
                .changeSource(source)
                .build()
                .oracleChangeLoader();
    }

    public static OracleChangeLoader of(PlatformStream stream) {
        return DaggerOraclePlatform.builder()
                .platformStream(stream)
                .changeSource(new ConfigFileSource())
                .build()
                .oracleChangeLoader();
    }

    public static OracleChangeSource ofSource(OracleConnection conn, Collection<String> tables) {
        Collection<String> tbls = Optional.ofNullable(tables)
                .filter(t -> !t.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("One or more tables required"));
        return new SimpleOracleChangeSource(conn, tbls);
    }
}
