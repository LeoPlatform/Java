package com.leo.schema;

import java.sql.Connection;
import java.util.List;

public interface ChangesSource {
    Connection connection();

    List<String> tables();
}
