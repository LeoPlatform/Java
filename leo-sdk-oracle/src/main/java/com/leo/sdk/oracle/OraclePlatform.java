package com.leo.sdk.oracle;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.LoadingStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.SDKPlatform;
import dagger.BindsInstance;
import dagger.Component;

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
        Builder loadingStream(LoadingStream stream);

        @BindsInstance
        Builder changeSource(OracleChangeSource source);

        OraclePlatform build();
    }

    OracleChangeWriter databaseChangeListener();
}
