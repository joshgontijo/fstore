package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;

public class SystemStreams {

    private static final String SEGMENTS = StreamName.SYSTEM_PREFIX + "segments";
    public static final String INDEX = StreamName.SYSTEM_PREFIX + "index";
    public static final String STREAMS = StreamName.SYSTEM_PREFIX + "streams";
    public static final String PROJECTIONS = StreamName.SYSTEM_PREFIX + "projections";
    public static final String ALL = StreamName.SYSTEM_PREFIX + "all";

    public static final long STREAMS_HASH = StreamName.hash(STREAMS);
    public static final long INDEX_HASH = StreamName.hash(INDEX);
    public static final long PROJECTIONS_HASH = StreamName.hash(PROJECTIONS);


}
