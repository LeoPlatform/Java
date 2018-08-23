package com.leo.sdk.oracle;

import com.leo.schema.ChangeDestination;

import java.util.Properties;

public interface OracleChangeDestination extends ChangeDestination {
    Properties getProps();
}
