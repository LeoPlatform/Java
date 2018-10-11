package io.leoplatform.sdk.oracle;

import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.SDKPlatform;

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
