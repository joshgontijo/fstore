package io.joshworks.eventry.stream;

import java.util.Optional;
import java.util.Set;

public class PartitionStreams {
    public long hashOf(String stream) {
        return 0;
    }

    public Optional<StreamMetadata> get(long streamHash) {
        return null;
    }

    public Optional<StreamMetadata> get(String stream) {
        return null;
    }

    public int version(long hash) {
        return 0;
    }

    public Set<String> streamMatching(String streamPattern) {
        return null;
    }

    public int tryIncrementVersion(long streamHash, int expectedVersion) {
        return 0;
    }

    public void truncate(String stream, int version) {


    }
}
