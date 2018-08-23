package com.leo.sdk.oracle;

import com.leo.schema.ChangeDestination;
import com.leo.schema.SimpleChangeListener;

import java.util.Properties;

public final class SimpleOracleChangeDestination implements OracleChangeDestination {

    private final ChangeDestination changeListener;
    private final Properties props;

    public SimpleOracleChangeDestination() {
        this(new SimpleChangeListener());
    }

    public SimpleOracleChangeDestination(ChangeDestination changeListener) {
        this(changeListener, new Properties());
    }

    public SimpleOracleChangeDestination(ChangeDestination changeListener, Properties props) {
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
