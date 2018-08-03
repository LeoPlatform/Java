package com.leo.sdk;

import java.util.stream.Stream;

public enum Uploader {
    STREAM("Stream"),
    STORAGE("Storage"),
    BATCH("Batch");

    private final String type;

    Uploader(String type) {
        this.type = type;
    }

    public String style() {
        return type;
    }

    public static Uploader fromType(String type) {
        return Stream.of(values())
                .filter(t -> t.style().equalsIgnoreCase(type))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploader style: " + type));
    }

    @Override
    public String toString() {
        return String.format("%s{style='%s'}", name(), type);
    }
}
