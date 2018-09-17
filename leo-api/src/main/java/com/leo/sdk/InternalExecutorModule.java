package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
public final class InternalExecutorModule {
    @Singleton
    @Provides
    @Named("Internal")
    static ExecutorManager provideExecutorManager(ConnectorConfig config) {
        return new InternalExecutorManager(config);
    }
}
