package io.leoplatform.sdk.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public final class StreamCorrelation {
    private final String source;
    private final String start;
    private final Long units;
    private final String end;

    public StreamCorrelation(String source, String start) {
        this(source, start, null, null);
    }

    public StreamCorrelation(String source, String start, Long units, String end) {
        this.source = source;
        this.start = start;
        this.units = units;
        this.end = end;
    }

    public String getSource() {
        return source;
    }

    public String getStart() {
        return start;
    }

    public Long getUnits() {
        return units;
    }

    public String getEnd() {
        return end;
    }
}
