package io.leoplatform.schema;

import java.sql.Connection;
import java.util.List;

public interface ChangeSource {
    Connection connection();

    List<String> tables();

    void end();
}
