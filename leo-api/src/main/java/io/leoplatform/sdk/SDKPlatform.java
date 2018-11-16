package io.leoplatform.sdk;

import dagger.Component;
import io.leoplatform.sdk.changes.ChangeReactor;
import io.leoplatform.sdk.changes.DomainResolver;
import io.leoplatform.sdk.changes.PayloadWriter;
import io.leoplatform.sdk.config.ConnectorConfig;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = SDKModule.class)
public interface SDKPlatform {

    ConnectorConfig connectorConfig();

    @Named("InternalExecutor")
    ExecutorManager executorManager();

    @Named("NullLoadingStream")
    LoadingStream loadingStream();

    @Named("NullOffloadingStream")
    OffloadingStream offloadingStream();

    @Named("SimpleDomainResolver")
    DomainResolver domainResolver();

    @Named("LeoChangeReactor")
    ChangeReactor changeReactor();

    @Named("BusPayloadWriter")
    PayloadWriter payloadWriter();
}
