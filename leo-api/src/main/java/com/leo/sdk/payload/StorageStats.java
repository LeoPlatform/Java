package com.leo.sdk.payload;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class StorageStats {
    @JsonIgnore
    private final Map<String, StorageUnits> botUnits;

    public StorageStats(Map<String, StorageUnits> botUnits) {
        this.botUnits = botUnits;
    }

    @JsonAnyGetter
    public Map<String, StorageUnits> any() {
        return botUnits;
    }
}
