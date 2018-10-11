package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.LITERAL;
import static java.util.stream.Collectors.toList;

public class ConfigFileSource implements OracleChangeSource {
    private static final String COLUMN_SEPARATOR = ",";
    private static final Pattern separatorPattern = Pattern.compile(COLUMN_SEPARATOR, LITERAL);
    private static final Config cfg = ConfigFactory.load("oracle_config.properties");

    @Override
    public OracleConnection connection() {
        OracleDriver dr = new OracleDriver();
        Properties props = new Properties();
        props.setProperty("user", cfg.getString("oracle.user"));
        props.setProperty("password", cfg.getString("oracle.pass"));
        try {
            Connection conn = dr.connect(cfg.getString("oracle.url"), props);
            return Optional.ofNullable(conn)
                    .map(OracleConnection.class::cast)
                    .orElseThrow(() -> new IllegalStateException("Unable to connect to database"));
        } catch (SQLException s) {
            throw new IllegalStateException("Fatal database error", s);
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
}
