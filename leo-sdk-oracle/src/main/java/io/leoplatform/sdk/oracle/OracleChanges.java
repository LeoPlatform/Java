package io.leoplatform.sdk.oracle;

import io.leoplatform.sdk.DaggerSDKPlatform;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.SDKPlatform;
import oracle.jdbc.OracleConnection;

import java.util.Collection;
import java.util.Optional;

public final class OracleChanges {
    public static OracleChangeLoader of(LoadingStream stream, OracleChangeSource source) {
        return DaggerOraclePlatform.builder()
                .loadingStream(stream)
                .changeSource(source)
                .build()
                .oracleChangeLoader();
    }

    public static OracleChangeLoader of(LoadingStream stream) {
        SDKPlatform sdkPlatform = DaggerSDKPlatform.builder()
                .build();

        return DaggerOraclePlatform.builder()
                .loadingStream(stream)
                .changeSource(new ConfigFileSource())
                .executorManager(sdkPlatform.executorManager())
                .build()
                .oracleChangeLoader();
    }

    public static OracleChangeSource ofSource(OracleConnection conn, Collection<String> tables) {
        Collection<String> tbls = Optional.ofNullable(tables)
                .filter(t -> !t.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("One or more tables required"));
        return new SimpleOracleChangeSource(conn, tbls);
    }
}
