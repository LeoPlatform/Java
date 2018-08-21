package com.leo.schema;

import java.sql.Connection;
import java.util.List;

public interface ChangeSource {
    Connection connection();

    List<String> tables();
}
