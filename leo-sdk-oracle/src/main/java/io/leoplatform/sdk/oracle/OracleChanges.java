package io.leoplatform.sdk.oracle;

import io.leoplatform.sdk.DaggerSDKPlatform;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.NullLoadingStream;
import io.leoplatform.sdk.changes.PayloadWriter;

public final class OracleChanges {
    public static OracleChangeLoader of(LoadingStream stream) {
        return DaggerOraclePlatform.builder()
            .loadingStream(stream)
            .executorManager(internalExecutor())
            .build()
            .oracleChangeLoader();
    }

    public static OracleChangeLoader ofLoader(LoadingStream stream) {
        return DaggerOraclePlatform.builder()
            .loadingStream(stream)
            .executorManager(internalExecutor())
            .build()
            .oracleChangeLoader();
    }

    public static OracleChangeLoader ofWriter(PayloadWriter writer) {
        return DaggerDomainObjectPlatform.builder()
            .loadingStream(new NullLoadingStream())
            .executorManager(internalExecutor())
            .payloadWriter(writer)
            .build()
            .oracleChangeLoader();
    }

    private static ExecutorManager internalExecutor() {
        return DaggerSDKPlatform.builder().build().executorManager();
    }
}
