package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.ChangeSource;
import oracle.jdbc.OracleConnection;

public interface OracleChangeSource extends ChangeSource {
    OracleConnection connection();
}
