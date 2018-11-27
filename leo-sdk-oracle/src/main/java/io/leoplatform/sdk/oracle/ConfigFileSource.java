package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.LITERAL;
import static java.util.stream.Collectors.toList;

@Singleton
public class ConfigFileSource implements OracleChangeSource {
    private static final Logger log = LoggerFactory.getLogger(ConfigFileSource.class);

    private static final String COLUMN_SEPARATOR = ",";
    private static final Pattern separatorPattern = Pattern.compile(COLUMN_SEPARATOR, LITERAL);
    private final Config oracleConfig;

    private OracleConnection conn;

    @Inject
    public ConfigFileSource(Config oracleConfig) {
        this.oracleConfig = oracleConfig;
        this.conn = getConnection();
    }

    @Override
    public OracleConnection connection() {
        try {
            return validConnection();
        } catch (Exception e) {
            log.info("Attempting to create connection to Oracle database");
            this.conn = getConnection();
            return validConnection();
        }
    }

    @Override
    public List<String> tables() {
        String tableStr = Optional.of(oracleConfig)
            .map(c -> c.getString("oracle.tables"))
            .orElseThrow(() -> new IllegalStateException("Missing oracle.tables key in oracle_config.properties"));

        return separatorPattern
            .splitAsStream(tableStr)
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(toList());
    }

    @Override
    public void end() {
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Unable to close connection", e);
        }
    }

    private OracleConnection getConnection() {
        OracleDriver dr = new OracleDriver();
        Properties props = new Properties();
        props.setProperty("user", oracleConfig.getString("oracle.user"));
        props.setProperty("password", oracleConfig.getString("oracle.pass"));
        try {
            String url = oracleConfig.getString("oracle.url");
            Connection conn = dr.connect(url, props);
            log.info("Established connection to {}", url);
            return Optional.ofNullable(conn)
                .map(OracleConnection.class::cast)
                .filter(OracleConnection::isUsable)
                .orElseThrow(() -> new IllegalStateException("Unable to connect to database"));
        } catch (SQLException s) {
            throw new IllegalStateException("Fatal database error", s);
        }
    }

    private OracleConnection validConnection() {
        return Optional.of(conn)
            .filter(OracleConnection::isUsable)
            .filter(this::canPing)
            .orElseThrow(() -> new IllegalStateException("Missing or invalid Oracle database connection"));
    }

    private boolean canPing(OracleConnection c) {
        try {
            return c.pingDatabase() == OracleConnection.DATABASE_OK;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot ping database", e);
        }
    }
}
