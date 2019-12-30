package io.joshworks;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.joshworks.ReplicatedLog.NO_SEQUENCE;

public class CommitTable {

    private final Map<String, Long> table = new ConcurrentHashMap<>();

    public void update(String nodeId, long lastSequence) {
        table.put(nodeId, lastSequence);
    }

    public long get(String nodeId) {
        return table.get(nodeId);
    }

    public long highWaterMark() {
        return table.values().stream().mapToLong(a -> a).min().orElse(NO_SEQUENCE);
    }

    public String highestSequenceNode() {
        return table.entrySet()
                .stream()
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElse(null);
    }


}
