package com.kong.assignment;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String LOCALHOST = "localhost";
    public static final String HTTP_PROTOCOL = "http";

    public static final String FILE_PATH = "stream.jsonl";

    public static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final String CDC_EVENTS_TOPIC = "cdc-events";
    public final static String CONSUMER_GROUP = "cdc-events-consumer-group";

    public static final int OPEN_SEARCH_PORT = 9200;
    public static final int MAX_RETRIES = 3;
    public static final String OPEN_SEARCH_CDC_INDEX = "cdc";
    public static final String DOCUMENT_ID = "id";


    public final static List<String> KONG_ENTITIES = Arrays.asList("service", "route", "node");
    public final static String USER_ID = "user_id";
    public final static String ENTITY_TYPE = "entity_type";


}
