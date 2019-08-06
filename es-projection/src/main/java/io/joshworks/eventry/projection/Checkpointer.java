package io.joshworks.eventry.projection;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.Closeable;
import java.io.File;
import java.util.Set;

public class Checkpointer implements Closeable {

    private static final String PROJECTIONS_DIR = "projection";
    private static final String PROJECTIONS_STORE_NAME = "projection-checkpoint";
    private final LsmTree<String, Checkpoint> store;

    public Checkpointer(File rootDir) {
        this.store = LsmTree.open(new File(rootDir, PROJECTIONS_DIR), Serializers.STRING, JsonSerializer.of(Checkpoint.class), 1000, PROJECTIONS_STORE_NAME);
    }

    public void checkpoint(String key, State state, Set<EventId> lastProvessed) {
        Checkpoint checkpoint = new Checkpoint(state, lastProvessed);
        store.put(key, checkpoint);
    }

    public Checkpoint get(String key) {
        return store.get(key);
    }

    public void remove(String key) {
        store.remove(key);
    }

    public static class Checkpoint {
        public final State state;
        public final Set<EventId> lastProcessed;

        private Checkpoint(State state, Set<EventId> lastProcessed) {
            this.state = state;
            this.lastProcessed = lastProcessed;
        }


    }

    @Override
    public void close() {
        store.close();
    }

}
