package com.leo.sdk.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public class StreamCorrelation {
    private final String source;
    private final Long start;
    private final Long units;
    private final Long end;

    public StreamCorrelation(String source, Long start) {
        this(source, start, null, null);
    }

    public StreamCorrelation(String source, Long start, Long units, Long end) {
        this.source = source;
        this.start = start;
        this.units = units;
        this.end = end;
    }

    public String getSource() {
        return source;
    }

    public Long getStart() {
        return start;
    }

    public Long getUnits() {
        return units;
    }

    public Long getEnd() {
        return end;
    }
}
