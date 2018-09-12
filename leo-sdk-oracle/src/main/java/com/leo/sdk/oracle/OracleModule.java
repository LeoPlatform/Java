package com.leo.sdk.oracle;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PlatformStream;
import dagger.Module;
import dagger.Provides;
import oracle.jdbc.dcn.DatabaseChangeListener;

import javax.inject.Singleton;

@Module
public final class OracleModule {
    @Singleton
    @Provides
    public static OracleChangeRegistrar provideOracleChangeRegistrar(OracleChangeSource source, DatabaseChangeListener dcl, ExecutorManager executorManager) {
        return new OracleChangeRegistrar(source, dcl, executorManager);
    }

    @Singleton
    @Provides
    public static OracleChangeLoader provideOracleChangeLoader(OracleChangeRegistrar registrar) {
        return new OracleChangeLoader(registrar);
    }

    @Singleton
    @Provides
    public static DatabaseChangeListener provideDatabaseChangeListener(PlatformStream stream, ExecutorManager executorManager) {
        return new OracleChangeWriter(stream, executorManager);
    }
}
