package io.leoplatform.sdk.oracle;

import io.leoplatform.sdk.ExecutorManager;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.function.ToLongFunction;

import static java.sql.ResultSet.FETCH_FORWARD;
import static oracle.jdbc.OracleConnection.*;

@Singleton
public final class OracleChangeRegistrar {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeRegistrar.class);

    private final OracleChangeSource source;
    private final OracleChangeWriter ocw;
    private final ExecutorManager executorManager;

    @Inject
    public OracleChangeRegistrar(OracleChangeSource source, OracleChangeWriter ocw, ExecutorManager executorManager) {
        this.source = source;
        this.ocw = ocw;
        this.executorManager = executorManager;
    }

    public DatabaseChangeRegistration create(OracleChangeDestination destination) {
        try (OracleConnection conn = source.connection()) {
            DatabaseChangeRegistration dcr = register(destination, conn);
            addChangeListener(dcr);
            addObjects(conn, dcr);
            increaseRowThreshold(conn);
            return dcr;
        } catch (SQLException e) {
            String msg = "'grant change notification to <User>' required for registration";
            log.error(msg);
            throw new IllegalStateException(msg, e);
        }
    }

    public List<String> remove(DatabaseChangeRegistration dcr) {
        try (OracleConnection conn = source.connection()) {
            conn.unregisterDatabaseChangeNotification(dcr);
            return source.tables();
        } catch (SQLException e) {
            log.warn("Unable to remove database change notification", e);
            return Collections.emptyList();
        }
    }

    public List<String> end() {
        ocw.end().join();
        executorManager.end();
        return source.tables();
    }

    private DatabaseChangeRegistration register(OracleChangeDestination destination, OracleConnection conn) throws SQLException {
        Properties props = listenerProps(destination);
        DatabaseChangeRegistration dcr = Optional
                .ofNullable(conn.registerDatabaseChangeNotification(props))
                .orElseThrow(() -> new SQLException("Missing change registration"));
        log.info("Registered listener from DB to {}:{}", props.get(NTF_LOCAL_HOST), props.get(NTF_LOCAL_TCP_PORT));
        return dcr;
    }

    private void addChangeListener(DatabaseChangeRegistration dcr) {
        try {
            dcr.addListener(ocw, executorManager.get());
        } catch (SQLException e) {
            throw new IllegalStateException("Could not add listener to registrar", e);
        }
    }

    private void increaseRowThreshold(OracleConnection conn) {
        try (CallableStatement cs = conn.prepareCall("{call DBMS_CQ_NOTIFICATION.SET_ROWID_THRESHOLD(?, ?)}")) {
            source.tables().forEach(t -> {
                try {
                    cs.setString(1, t);
                    cs.setInt(2, 100_000);
                    cs.executeUpdate();
                } catch (SQLException s) {
                    throw new IllegalStateException("Could not set threshold parameter " + t, s);
                }
            });
        } catch (SQLException s) {
            throw new IllegalStateException("Could not set ROWID threshold to 10,000", s);
        }
    }

    private void addObjects(OracleConnection conn, DatabaseChangeRegistration dcr) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
            stmt.setFetchSize(100_000);
            stmt.setFetchDirection(FETCH_FORWARD);
            registerTables(stmt);
        } catch (SQLException s) {
            String msg = "Could not register tables tables for notification";
            log.warn(msg, s);
            conn.unregisterDatabaseChangeNotification(dcr);
            throw s;
        }
    }

    private void registerTables(Statement stmt) {
        long totalEntries = source.tables().stream()
                .map(t -> consume(stmt, t))
                .mapToLong(getRows())
                .sum();
        int numTables = source.tables().size();
        String entriesLbl = totalEntries == 1 ? "entry" : "entries";
        String tablesLbl = numTables == 1 ? "table" : "tables";
        log.info("Watching {} {} with a total of {} {}", numTables, tablesLbl, totalEntries, entriesLbl);
    }

    private Entry<String, Long> consume(Statement stmt, String table) {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
            long entries = 0;
            while (rs.next()) {
                entries++;
            }
            return new SimpleImmutableEntry<>(table, entries);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not read from registered table " + table, e);
        }
    }

    private Properties listenerProps(OracleChangeDestination destination) {
        Properties p = new Properties(destination.getProps());
        p.put(DCN_NOTIFY_ROWIDS, "true");
        p.put(NTF_QOS_RELIABLE, "true");
        p.put(NTF_LOCAL_HOST, destination.getHost());
        p.put(NTF_LOCAL_TCP_PORT, String.valueOf(destination.getPort()));
        return p;
    }

    private ToLongFunction<Entry<String, Long>> getRows() {
        return res -> {
            String entriesLbl = res.getValue() == 1 ? "entry" : "entries";
            log.info("Watching for changes to {} with {} {}", res.getKey(), res.getValue(), entriesLbl);
            return res.getValue();
        };
    }
}
