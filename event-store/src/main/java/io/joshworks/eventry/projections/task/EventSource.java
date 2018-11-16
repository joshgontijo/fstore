package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.log.EventRecord;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

interface EventSource extends Closeable {

    Set<String> streams();

    EventRecord next();

    boolean hasNext();

    long position();

    Map<String, Integer> streamTracker();
}
