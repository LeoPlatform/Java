package com.leo.sdk.oracle;

import com.leo.schema.ChangesSource;
import oracle.jdbc.OracleConnection;

public interface OracleChangesSource extends ChangesSource {
    OracleConnection connection();
}
