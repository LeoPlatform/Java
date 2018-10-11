package io.leoplatform.sdk.oracle;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;

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
    public static OracleChangeWriter provideDatabaseChangeListener(LoadingStream stream, ExecutorManager executorManager) {
        return new OracleChangeWriter(stream, executorManager);
    }
}
