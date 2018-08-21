package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import oracle.jdbc.OracleConnection;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

public final class OracleChanges {
    public static OracleChangeSource of(OracleConnection conn, Stream<String> tables) {
        return new SimpleOracleChangeSource(conn, tables);
    }

    public static OracleChangeSource of(OracleConnection conn, Collection<String> tables) {
        Stream<String> tbls = Optional.ofNullable(tables)
                .map(Collection::stream)
                .orElseThrow(() -> new IllegalArgumentException("One or more tables required"));
        return OracleChanges.of(conn, tbls);
    }

    public static OracleChangeLoader ofLoader(OracleChangeSource source, PlatformStream destination) {
        return OracleChanges.ofLoader(source, new SimpleOracleChangeListener(), destination);
    }

    public static OracleChangeLoader ofLoader(OracleChangeSource source, OracleChangeListener listener, PlatformStream destination) {
        return new OracleChangeLoader(source, listener, destination);
    }

    public static OracleChangeLoader ofLoader(OracleChangeSource source, PlatformStream destination, Executor executor) {
        return OracleChanges.ofLoader(source, new SimpleOracleChangeListener(), destination, executor);
    }

    public static OracleChangeLoader ofLoader(OracleChangeSource source, OracleChangeListener listener, PlatformStream destination, Executor executor) {
        return new OracleChangeLoader(source, listener, destination, executor);
    }
}
