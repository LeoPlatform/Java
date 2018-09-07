package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import dagger.BindsInstance;
import dagger.Component;
import oracle.jdbc.dcn.DatabaseChangeListener;

@Component(modules = {OracleModule.class, SDKModule.class})
public interface OraclePlatform {

    OracleChangeRegistrar oracleChangeRegistrar();

    OracleChangeLoader oracleChangeLoader();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder platformStream(PlatformStream stream);

        @BindsInstance
        Builder changeSource(OracleChangeSource source);

        OraclePlatform build();
    }

    DatabaseChangeListener databaseChangeListener();
}
