package io.leoplatform.sdk.aws.s3;

import java.util.stream.Stream;

public enum S3BufferStyle {
    MEMORY("Memory"), DISK("Disk");

    private final String style;

    S3BufferStyle(String style) {
        this.style = style;
    }

    @Override
    public String toString() {
        return style;
    }

    static S3BufferStyle fromName(String style) {
        return Stream.of(values())
                .filter(s -> s.toString().equalsIgnoreCase(style))
                .findFirst()
                .orElse(MEMORY);
    }
}
