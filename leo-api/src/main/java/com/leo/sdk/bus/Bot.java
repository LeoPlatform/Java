package com.leo.sdk.bus;

import java.util.Collections;
import java.util.List;

public interface Bot {
    String name();

    default List<String> tags() {
        return Collections.emptyList();
    }

    default String description() {
        return "";
    }
}
