package com.leo.sdk.oracle;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.SDKPlatform;
import dagger.BindsInstance;
import dagger.Component;
import oracle.jdbc.dcn.DatabaseChangeListener;

import javax.inject.Singleton;

@Singleton
@Component(modules = {OracleModule.class, SDKModule.class})
public interface OraclePlatform extends SDKPlatform {

    OracleChangeRegistrar oracleChangeRegistrar();

    OracleChangeLoader oracleChangeLoader();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        @BindsInstance
        Builder platformStream(PlatformStream stream);

        @BindsInstance
        Builder changeSource(OracleChangeSource source);

        OraclePlatform build();
    }

    DatabaseChangeListener databaseChangeListener();
}
