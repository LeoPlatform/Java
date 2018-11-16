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

    public static OracleChangeLoader ofLoader(PayloadWriter writer) {
        return DaggerOraclePlatform.builder()
            .loadingStream(new NullLoadingStream())
            .executorManager(internalExecutor())
            .build()
            .oracleChangeLoader();
    }

    private static ExecutorManager internalExecutor() {
        return DaggerSDKPlatform.builder().build().executorManager();
    }
}
