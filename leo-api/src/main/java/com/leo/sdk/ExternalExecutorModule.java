package com.leo.sdk;

import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.Executor;

@Module
public final class ExternalExecutorModule {
    @Singleton
    @Provides
    @Named("External")
    static ExecutorManager provideExecutorManager(Executor executor) {
        return new ExternalExecutorManager(executor);
    }
}
