package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
    private static final Config cfg = getCfg();

    private OracleConnection conn;

    @Inject
    public ConfigFileSource() {
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
        String tableStr = Optional.of(cfg)
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
        props.setProperty("user", cfg.getString("oracle.user"));
        props.setProperty("password", cfg.getString("oracle.pass"));
        try {
            String url = cfg.getString("oracle.url");
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
            .filter(this::pingable)
            .orElseThrow(() -> new IllegalStateException("Missing or invalid Oracle database connection"));
    }

    private boolean pingable(OracleConnection c) {
        try {
            return c.pingDatabase() == OracleConnection.DATABASE_OK;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot ping database", e);
        }
    }

    private static Config getCfg() {
        //TODO: Move these into the injector config
        Config cfg = ConfigFactory.load("oracle_config.properties");
        System.setProperty("LEO.CHANGE_USER", cfg.getString("oracle.user"));
        System.setProperty("LEO.CHANGE_PASS", cfg.getString("oracle.pass"));
        System.setProperty("LEO.CHANGE_URL", cfg.getString("oracle.url"));
        System.setProperty("LEO.CHANGE_TABLES", cfg.getString("oracle.tables"));
        return cfg;
    }
}
