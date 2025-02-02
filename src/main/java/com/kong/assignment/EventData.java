package com.kong.assignment;

import lombok.Data;

import java.util.Map;

@Data
public class EventData {
    private String before;
    private After after;
    private String op;
    private long ts_ms;
}

@Data
class Value {
    private int type;
    private Map<String, Object> object;
}

@Data
class After {
    private String key;
    private Value value;
}

