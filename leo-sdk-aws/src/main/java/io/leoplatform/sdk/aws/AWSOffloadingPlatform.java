package io.leoplatform.sdk.aws;

import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.OffloadingStream;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.bus.OffloadingBot;

import javax.inject.Named;
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

    @Named("AwsOffloadingStream")
    OffloadingStream offloadingStream();
}
