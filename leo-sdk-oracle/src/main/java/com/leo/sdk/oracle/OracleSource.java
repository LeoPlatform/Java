package com.leo.sdk.oracle;

import oracle.jdbc.OracleConnection;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class OracleSource implements OracleChangesSource {

    private final OracleConnection conn;
    private final List<String> tables;

    public OracleSource(OracleConnection conn, List<String> tables) {
        this.conn = Optional.ofNullable(conn)
                .filter(this::pingable)
                .orElseThrow(() -> new IllegalStateException("Missing or invalid connection"));
        this.tables = Optional.ofNullable(tables)
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
