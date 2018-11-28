package io.leoplatform.sdk;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.schema.ChangeSource;
import io.leoplatform.sdk.changes.*;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.config.FileConfig;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.Executor;

@Module
public class SDKModule {
    @Singleton
    @Provides
    public static ConnectorConfig provideConnectorConfig() {
        return new FileConfig();
    }

    @Singleton
    @Provides
    @Named("InternalExecutor")
    public static ExecutorManager provideInternalExecutorManager(ConnectorConfig config) {
        return new InternalExecutorManager(config);
    }

    @Singleton
    @Provides
    @Named("ExternalExecutor")
    public static ExecutorManager provideExternalExecutorManager(Executor executor) {
        return new ExternalExecutorManager(executor);
    }

    @Singleton
    @Provides
    @Named("NullLoadingStream")
    public static LoadingStream provideNullLoadingStream() {
        return new NullLoadingStream();
    }

    @Singleton
    @Provides
    @Named("NullOffloadingStream")
    public static OffloadingStream provideNullOffloadingStream() {
        return new NullOffloadingStream();
    }

    @Singleton
    @Provides
    @Named("SimpleDomainResolver")
    public static DomainResolver provideDomainResolver() {
        return new SimpleDomainResolver();
    }

    @Singleton
    @Provides
    @Named("LeoChangeReactor")
    public static ChangeReactor provideChangeReactor() {
        return new LeoChangesPayload();
    }

    @Singleton
    @Provides
    @Named("BusPayloadWriter")
    public static PayloadWriter provideBusPayloadWriter(@Named("NullLoadingStream") LoadingStream stream) {
        return new BusWriter(stream);
    }

    @Singleton
    @Provides
    public static JsonDomainData provideJsonDomainData(ChangeSource changeSource) {
        return new JacksonDomainJson(changeSource);
    }
}
