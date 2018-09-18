package com.leo.sdk.oracle;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PlatformStream;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public final class OracleModule {
    @Singleton
    @Provides
    public static OracleChangeRegistrar provideOracleChangeRegistrar(OracleChangeSource source, OracleChangeWriter ocw, ExecutorManager executorManager) {
        return new OracleChangeRegistrar(source, ocw, executorManager);
    }

    @Singleton
    @Provides
    public static OracleChangeLoader provideOracleChangeLoader(OracleChangeRegistrar registrar) {
        return new OracleChangeLoader(registrar);
    }

    @Singleton
    @Provides
    public static OracleChangeWriter provideDatabaseChangeListener(PlatformStream stream, ExecutorManager executorManager) {
        return new OracleChangeWriter(stream, executorManager);
    }
}
