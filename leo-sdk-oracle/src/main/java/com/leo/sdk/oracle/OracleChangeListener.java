package com.leo.sdk.oracle;

import com.leo.schema.ChangeListener;

import java.util.Properties;

public interface OracleChangeListener extends ChangeListener {
    Properties getProps();
}
