package io.leoplatform.sdk.bus;

import javax.inject.Singleton;

@Singleton
public interface LoadingBot extends Bot {
    StreamQueue destination();
}
