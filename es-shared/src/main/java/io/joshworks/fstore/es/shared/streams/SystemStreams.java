package io.joshworks.fstore.es.shared.streams;

import io.joshworks.fstore.es.shared.EventId;

public class SystemStreams {

    private static final String SEGMENTS = EventId.SYSTEM_PREFIX + "segments";
    public static final String INDEX = EventId.SYSTEM_PREFIX + "index";
    public static final String STREAMS = EventId.SYSTEM_PREFIX + "streams";
    public static final String PROJECTIONS = EventId.SYSTEM_PREFIX + "projections";
    public static final String ALL = EventId.SYSTEM_PREFIX + "all";

    public static final long STREAMS_HASH = EventId.hash(STREAMS);
    public static final long INDEX_HASH = EventId.hash(INDEX);
    public static final long PROJECTIONS_HASH = EventId.hash(PROJECTIONS);


}
