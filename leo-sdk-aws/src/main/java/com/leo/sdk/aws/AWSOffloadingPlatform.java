package com.leo.sdk.aws;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.OffloadingStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.bus.OffloadingBot;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Singleton;

@Singleton
@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSOffloadingPlatform extends AWSPlatform {
    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        @BindsInstance
        Builder offloadingBot(OffloadingBot offloadingBot);

        AWSOffloadingPlatform build();
    }

    OffloadingStream offloadingStream();
}
