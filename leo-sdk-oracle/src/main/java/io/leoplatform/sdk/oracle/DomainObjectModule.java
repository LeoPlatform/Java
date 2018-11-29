package io.leoplatform.sdk.oracle;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
    public static Config provideOracleConfig() {
        return ConfigFactory.load("oracle_config.properties");
    }

    @Singleton
    @Provides
    public static SchemaChangeQueue provideSchemaChangeQueue(@Named("DomainObjectReactor") ChangeReactor changeReactor, ExecutorManager executorManager) {
        return new AsyncChangeQueue(changeReactor, executorManager);
    }

    @Singleton
    @Provides
    public static OracleChangeSource provideOracleChangeSource(Config oracleConfig) {
        return new ConfigFileSource(oracleConfig);
    }

    @Singleton
    @Provides
    public static ChangeSource provideChangeSource(Config oracleConfig) {
        return new PooledChangeSource(oracleConfig);
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
    public static OracleChangeWriter provideOracleChangeWriter(SchemaChangeQueue changeQueue, ExecutorManager executorManager) {
        return new OracleChangeWriter(changeQueue, executorManager);
    }

    @Singleton
    @Provides
    public static DomainQuery provideDomainQuery(Config oracleConfig) {
        return new OracleDomainQuery(oracleConfig);
    }

    @Singleton
    @Provides
    @Named("DomainObjectResolver")
    public static DomainResolver provideDomainObjectResolver(DomainQuery domainQuery, JsonDomainData domainData, ExecutorManager executorManager) {
        return new OracleRowResolver(domainQuery, domainData, executorManager);
    }

    @Singleton
    @Provides
    @Named("DomainObjectReactor")
    public static ChangeReactor provideDomainObjectReactor(@Named("DomainObjectResolver") DomainResolver domainResolver, PayloadWriter payloadWriter) {
        return new DomainObjectPayload(domainResolver, payloadWriter);
    }
}
