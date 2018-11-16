package io.leoplatform.sdk.oracle;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.schema.ChangeSource;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.changes.*;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
public final class DomainObjectModule {
    @Singleton
    @Provides
    public static OracleChangeSource provideOracleChangeSource() {
        return new ConfigFileSource();
    }

    @Singleton
    @Provides
    public static ChangeSource provideChangeSource() {
        return new PooledChangeSource();
    }

    @Singleton
    @Provides
    public static OracleChangeRegistrar provideOracleChangeRegistrar(OracleChangeSource source, OracleChangeWriter ocw, ExecutorManager executorManager) {
        return new OracleChangeRegistrar(source, ocw, executorManager);
    }

    @Singleton
    @Provides
    public static OracleChangeLoader provideOracleChangeLoader(OracleChangeRegistrar registrar) {
        return new OracleChangeLoader(registrar);
    }

    @Singleton
    @Provides
    @Named("DomainObjectResolver")
    public static DomainResolver provideDomainObjectResolver(ChangeSource source, JsonDomainData domainData) {
        return new OracleRowResolver(source, domainData);
    }

    @Singleton
    @Provides
    @Named("DomainObjectReactor")
    public static ChangeReactor provideDomainObjectReactor(@Named("DomainObjectResolver") DomainResolver domainResolver, PayloadWriter payloadWriter) {
        return new DomainObjectPayload(domainResolver, payloadWriter);
    }

    @Singleton
    @Provides
    public static SchemaChangeQueue provideSchemaChangeQueue(@Named("DomainObjectReactor") ChangeReactor changeReactor, ExecutorManager executorManager) {
        return new AsyncChangeQueue(changeReactor, executorManager);
    }
}
