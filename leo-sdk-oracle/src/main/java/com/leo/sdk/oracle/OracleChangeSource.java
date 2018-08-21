package com.leo.sdk.oracle;

import com.leo.schema.ChangeSource;
import oracle.jdbc.OracleConnection;

public interface OracleChangeSource extends ChangeSource {
    OracleConnection connection();
}
