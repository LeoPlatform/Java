package com.leo.sdk.payload;

import java.util.List;

public interface StoragePayload {
    String getEvent();

    String getStart();

    String getEnd();

    List<StorageEventOffset> getOffsets();

    Long getGzipSize();

    Long getSize();

    Long getRecords();

    List<StreamCorrelation> getCorrelations();
}
