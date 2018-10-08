package io.joshworks.eventry.projections;

import java.util.Set;

public class StreamSource {

    public final Set<String> streams;
    public final boolean parallel;

    public StreamSource(Set<String> streams, boolean parallel) {
        this.streams = streams;
        this.parallel = parallel;
    }

    public boolean isSingleSource() {
        //zipped streams is single source
        return streams.size() == 1 || !parallel;
    }

}
