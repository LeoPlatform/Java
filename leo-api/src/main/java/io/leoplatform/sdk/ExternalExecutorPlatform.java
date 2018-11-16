package io.leoplatform.sdk;

import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.Executor;

@Singleton
@Component(modules = {SDKModule.class})
public interface ExternalExecutorPlatform extends SDKPlatform {
    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executor(Executor executor);

        ExternalExecutorPlatform build();
    }

    @Named("ExternalExecutor")
    ExecutorManager executorManager();
}
