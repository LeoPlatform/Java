package com.leo.sdk.bus;

import java.util.Collections;
import java.util.List;

public interface StreamQueue {
    String name();

    default List<String> tags() {
        return Collections.emptyList();
    }
}
