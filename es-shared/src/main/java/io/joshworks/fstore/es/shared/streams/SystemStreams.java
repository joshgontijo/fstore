package io.joshworks.fstore.es.shared.streams;

import io.joshworks.fstore.es.shared.EventId;

import java.util.Set;

public class SystemStreams {

    public static final String INDEX = EventId.SYSTEM_PREFIX + "index";
    public static final String STREAMS = EventId.SYSTEM_PREFIX + "streams";
    public static final String PROJECTIONS = EventId.SYSTEM_PREFIX + "projections";
    public static final String ALL = EventId.SYSTEM_PREFIX + "all";

    public static final long STREAMS_HASH = StreamHasher.hash(STREAMS);
    public static final long INDEX_HASH = StreamHasher.hash(INDEX);
    public static final long PROJECTIONS_HASH = StreamHasher.hash(PROJECTIONS);
    public static final long ALL_HASH = StreamHasher.hash(ALL);

    private static final Set<Long> HASHES = Set.of(STREAMS_HASH, INDEX_HASH, PROJECTIONS_HASH, ALL_HASH);


    public static boolean systemStream(long hash) {
        return HASHES.contains(hash);
    }

}
