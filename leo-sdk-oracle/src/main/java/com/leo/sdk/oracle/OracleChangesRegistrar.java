package com.leo.sdk.oracle;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.DatabaseChangeRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.jdbc.OracleConnection.NTF_LOCAL_HOST;
import static oracle.jdbc.OracleConnection.NTF_LOCAL_TCP_PORT;

public class OracleChangesRegistrar {
    private static final Logger log = LoggerFactory.getLogger(OracleChangesRegistrar.class);

    private final OracleChangesSource source;
    private final OracleChangeListener listener;

    private final Lock lock = new ReentrantLock();
    private final Condition running = lock.newCondition();
    private volatile boolean listening = false;
    private Instant start = null;
    private Instant end = null;

    public OracleChangesRegistrar(OracleChangesSource source, OracleChangeListener listener) {
        this.source = source;
        this.listener = listener;
    }

    public Runnable listen(DatabaseChangeListener changeListener, Executor executor) {
        if (listening) {
            return () -> log.warn("Attempt to start listener multiple times on port {}", listener.getPort());
        }

        return () -> {
            DatabaseChangeRegistration dcr = createChangeRegistrar();
            addListener(dcr, changeListener, executor);
            try {
                register(dcr);
                keepAlive();
            } finally {
                deregister(dcr);
            }
            log.info("Listened for Oracle changes for {} seconds", Duration.between(start, Instant.now()).getSeconds());
        };
    }

    public WatchResults end() {
        lock.lock();
        try {
            listening = false;
            running.signalAll();
        } finally {
            lock.unlock();
        }
        log.info("Change listener stopped");
        return new WatchResults(start, end, source.tables());
    }

    private void addListener(DatabaseChangeRegistration dcr, DatabaseChangeListener changeListener, Executor executor) {
        try {
            dcr.addListener(changeListener, executor);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to register listener", e);
        }
    }

    private void keepAlive() {
        lock.lock();
        try {
            do {
                if (running.await(30L, SECONDS)) {
                    ping();
                }
            } while (listening);
        } catch (InterruptedException e) {
            log.info("Change listener shutting down");
        } finally {
            lock.unlock();
        }
    }

    private void ping() {
        try {
            source.connection().pingDatabase();
        } catch (SQLException e) {
            log.warn("Could not ping database", e);
        }
    }

    private void register(DatabaseChangeRegistration dcr) {
        lock.lock();
        try (Statement stmt = source.connection().createStatement()) {
            ((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
            registerTables(stmt);
            listening = true;
            start = Instant.now();
        } catch (SQLException s) {
            String msg = "Could not register tables tables for notification";
            log.warn(msg, s);
            listening = false;
            throw new IllegalArgumentException(msg, s);
        } finally {
            lock.unlock();
        }
    }

    private void deregister(DatabaseChangeRegistration dcr) {
        lock.lock();
        try {
            try {
                source.connection().unregisterDatabaseChangeNotification(dcr);
            } catch (SQLException e) {
                log.warn("Unable to unregister change notification", e);
            }
            try {
                source.connection().close();
            } catch (SQLException e) {
                log.warn("Unable to close Oracle connection", e);
            }
            end = Instant.now();
        } finally {
            lock.unlock();
        }
    }

    private DatabaseChangeRegistration createChangeRegistrar() {
        Properties props = listenerProps();
        try {
            DatabaseChangeRegistration dcr = Optional
                    .ofNullable(source.connection().registerDatabaseChangeNotification(props))
                    .orElseThrow(() -> new SQLException("Missing change registration"));
            log.info("Created listener from DB {} on port {}", props.get(NTF_LOCAL_HOST), props.get(NTF_LOCAL_TCP_PORT));
            return dcr;
        } catch (SQLException e) {
            String msg = "'grant change notification to <User>' required for registration";
            log.error(msg);
            throw new IllegalStateException(msg, e);
        }
    }

    private void registerTables(Statement stmt) {
        source.tables().parallelStream()
                .map(t -> consume(stmt, t))
                .forEach(logResults());
    }

    private Entry<String, Long> consume(Statement stmt, String table) {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
            long entries = 0;
            while (rs.next()) {
                entries++;
            }
            return new SimpleImmutableEntry<>(table, entries);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not read from watched table", e);
        }
    }

    private Properties listenerProps() {
        Properties p = new Properties(listener.getProps());
        p.put(NTF_LOCAL_HOST, listener.getHost());
        p.put(NTF_LOCAL_TCP_PORT, String.valueOf(listener.getPort()));
        p.put(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        return p;
    }

    private Consumer<Entry<String, Long>> logResults() {
        return res -> log.info("Watching for changes to {} with {} entries", res.getKey(), res.getValue());
    }
}
