package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import dagger.Module;
import dagger.Provides;
import oracle.jdbc.dcn.DatabaseChangeListener;

@Module
public final class OracleModule {
    @Provides
    public static OracleChangeRegistrar provideOracleChangeRegistrar(OracleChangeSource source, DatabaseChangeListener dcl) {
        return new OracleChangeRegistrar(source, dcl);
    }

    @Provides
    public static OracleChangeLoader provideOracleChangeLoader(OracleChangeRegistrar registrar) {
        return new OracleChangeLoader(registrar);
    }

    @Provides
    public static DatabaseChangeListener provideDatabaseChangeListener(PlatformStream stream) {
        return new OracleChangeWriter(stream);
    }
}
