package com.leo.sdk.oracle;

import com.leo.sdk.DaggerSDKPlatform;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKPlatform;
import oracle.jdbc.OracleConnection;

import java.util.Collection;
import java.util.Optional;

public final class OracleChanges {
    public static OracleChangeLoader of(PlatformStream stream, OracleChangeSource source) {
        return DaggerOraclePlatform.builder()
                .platformStream(stream)
                .changeSource(source)
                .build()
                .oracleChangeLoader();
    }

    public static OracleChangeLoader of(PlatformStream stream) {
        SDKPlatform sdkPlatform = DaggerSDKPlatform.builder()
                .build();

        return DaggerOraclePlatform.builder()
                .platformStream(stream)
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
