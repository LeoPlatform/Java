package com.leo.sdk.oracle;

import oracle.jdbc.OracleConnection;

import java.sql.SQLException;
import java.util.*;

import static java.util.stream.Collectors.toList;

public final class SimpleOracleChangeSource implements OracleChangeSource {

    private final OracleConnection conn;
    private final List<String> tables;

    public SimpleOracleChangeSource(OracleConnection conn, Collection<String> tables) {
        this.conn = Optional.ofNullable(conn)
                .filter(this::pingable)
                .orElseThrow(() -> new IllegalStateException("Missing or invalid connection"));
        this.tables = validate(tables);
    }

    private List<String> validate(Collection<String> tables) {
        List<String> tbls = Optional.ofNullable(tables)
                .orElse(Collections.emptyList())
                .stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(t -> !t.isEmpty())
                .collect(toList());
        return Optional.of(tbls)
                .filter(t -> !t.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Must watch one or more tables"));
    }

    private boolean pingable(OracleConnection c) {
        try {
            return c.pingDatabase() == OracleConnection.DATABASE_OK;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot ping database", e);
        }
    }

    @Override
    public OracleConnection connection() {
        return conn;
    }

    @Override
    public List<String> tables() {
        return tables;
    }
}
