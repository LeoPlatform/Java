package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.ChangeDestination;

import java.util.Properties;

public interface OracleChangeDestination extends ChangeDestination {
    Properties getProps();
}
