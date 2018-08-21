package com.leo.sdk.oracle;

import com.leo.schema.ChangeListener;
import com.leo.schema.SimpleChangeListener;

import java.util.Properties;

public final class SimpleOracleChangeListener implements OracleChangeListener {

    private final ChangeListener changeListener;
    private final Properties props;

    public SimpleOracleChangeListener() {
        this(new SimpleChangeListener());
    }

    public SimpleOracleChangeListener(ChangeListener changeListener) {
        this(changeListener, new Properties());
    }

    public SimpleOracleChangeListener(ChangeListener changeListener, Properties props) {
        this.changeListener = changeListener;
        this.props = props;
    }

    @Override
    public String getHost() {
        return changeListener.getHost();
    }

    @Override
    public Integer getPort() {
        return changeListener.getPort();
    }

    @Override
    public Properties getProps() {
        return props;
    }
}
