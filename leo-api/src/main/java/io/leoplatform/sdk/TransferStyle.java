package io.leoplatform.sdk;

import java.util.stream.Stream;

public enum TransferStyle {
    STREAM("Stream"),
    STORAGE("Storage"),
    BATCH("Batch"),
    PROXY("Proxy");

    private final String style;

    TransferStyle(String style) {
        this.style = style;
    }

    public String style() {
        return style;
    }

    public static TransferStyle fromType(String style) {
        return Stream.of(values())
                .filter(t -> t.style().equalsIgnoreCase(style))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Unknown writer style: " + style));
    }

    @Override
    public String toString() {
        return String.format("%s{style='%s'}", name(), style);
    }
}
