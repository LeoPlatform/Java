package io.leoplatform.sdk.oracle;

import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.schema.ChangeSource;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.SDKPlatform;
import io.leoplatform.sdk.changes.SchemaChangeQueue;

import javax.inject.Singleton;

@Singleton
@Component(modules = {SDKModule.class, OracleModule.class})
public interface OraclePlatform extends SDKPlatform {

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        @BindsInstance
        Builder loadingStream(LoadingStream stream);

        OraclePlatform build();
    }

    SchemaChangeQueue changeQueue();

    OracleChangeSource oracleChangeSource();

    ChangeSource changeSource();

    OracleChangeRegistrar oracleChangeRegistrar();

    OracleChangeLoader oracleChangeLoader();

    OracleChangeWriter oracleChangeWriter();
}
