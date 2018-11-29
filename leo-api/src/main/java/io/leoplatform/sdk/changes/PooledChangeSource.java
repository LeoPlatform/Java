package io.leoplatform.sdk.changes;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.leoplatform.schema.ChangeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.LITERAL;
import static java.util.stream.Collectors.toList;

@Singleton
public class PooledChangeSource implements ChangeSource {
    private static final Logger log = LoggerFactory.getLogger(PooledChangeSource.class);

    private final Config oracleConfig;
    private final HikariDataSource ds;
    private final List<String> tables;

    @Inject
    public PooledChangeSource(Config oracleConfig) {
        this.oracleConfig = oracleConfig;
        this.ds = new HikariDataSource(fromConfig());
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

    private HikariConfig fromConfig() {
        Properties props = new Properties() {{
            setProperty("dataSource.user", oracleConfig.getString("oracle.user"));
            setProperty("dataSource.password", oracleConfig.getString("oracle.pass"));
            setProperty("jdbcUrl", oracleConfig.getString("oracle.url"));
        }};
        return new HikariConfig(props);
    }

    private List<String> parseTables() {
        String tableStr = oracleConfig.getString("oracle.tables");

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
