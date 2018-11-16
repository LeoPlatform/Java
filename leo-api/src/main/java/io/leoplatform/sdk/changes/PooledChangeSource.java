package io.leoplatform.sdk.changes;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.leoplatform.schema.ChangeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.LITERAL;
import static java.util.stream.Collectors.toList;

public class PooledChangeSource implements ChangeSource {
    private static final Logger log = LoggerFactory.getLogger(PooledChangeSource.class);

    private final HikariDataSource ds;
    private final List<String> tables;

    @Inject
    public PooledChangeSource() {
        this.ds = new HikariDataSource(new HikariConfig(fromEnvironment()));
        this.tables = parseTables();
    }

    @Override
    public Connection connection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            log.warn("Unable to retrieve connection", e);
            throw new IllegalStateException("Not an OracleConnection instance");
        }
    }

    @Override
    public List<String> tables() {
        return tables;
    }

    private Properties fromEnvironment() {
        Properties props = new Properties();
        props.setProperty("dataSource.user", validate("LEO.CHANGE_USER"));
        props.setProperty("dataSource.password", validate("LEO.CHANGE_PASS"));
        props.setProperty("jdbcUrl", validate("LEO.CHANGE_URL"));
        return props;
    }

    private String validate(String envVar) {
        return Optional.ofNullable(System.getProperty(envVar))
            .filter(e -> !e.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Missing " + envVar + " in environment"));
    }

    private List<String> parseTables() {
        String tableStr = validate("LEO.CHANGE_TABLES");

        return Pattern.compile(",", LITERAL)
            .splitAsStream(tableStr)
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(toList());
    }

    @Override
    public void end() {
        ds.close();
    }
}
