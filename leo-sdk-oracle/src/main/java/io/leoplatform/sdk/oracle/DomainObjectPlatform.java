package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.schema.ChangeSource;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.changes.*;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = {SDKModule.class, DomainObjectModule.class})
public interface DomainObjectPlatform extends OraclePlatform {

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        @BindsInstance
        Builder loadingStream(LoadingStream loadingStream);

        @BindsInstance
        Builder payloadWriter(PayloadWriter payloadWriter);

        DomainObjectPlatform build();
    }

    SchemaChangeQueue changeQueue();

    OracleChangeRegistrar oracleChangeRegistrar();

    OracleChangeLoader oracleChangeLoader();

    OracleChangeSource oracleChangeSource();

    ChangeSource changeSource();

    Config oracleConfig();

    DomainQuery domainQuery();

    @Named("DomainObjectResolver")
    DomainResolver domainResolver();

    @Named("DomainObjectReactor")
    ChangeReactor changeReactor();
}
