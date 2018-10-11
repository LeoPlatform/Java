package io.leoplatform.sdk.bus;

import javax.inject.Singleton;

@Singleton
public interface OffloadingBot extends Bot {
    StreamQueue source();
}
