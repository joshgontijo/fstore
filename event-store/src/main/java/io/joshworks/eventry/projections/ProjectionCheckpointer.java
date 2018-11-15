package io.joshworks.eventry.projections;

import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.Closeable;
import java.io.File;
import java.util.Map;

public class ProjectionCheckpointer implements Closeable {

    private static final String PROJECTIONS_DIR = "projection";
    private static final String PROJECTIONS_STORE_NAME = "projection-checkpointer";
    private final LsmTree<String, ProjectionCheckpoint> store;

    public ProjectionCheckpointer(File rootDir) {
        this.store = LsmTree.open(new File(rootDir, PROJECTIONS_DIR), Serializers.STRING, JsonSerializer.of(ProjectionCheckpoint.class), 1000, PROJECTIONS_STORE_NAME);
    }

    public void checkpoint(String key, State state, Map<String, Integer> tracker) {
        ProjectionCheckpoint checkpoint = new ProjectionCheckpoint(state, tracker);
        store.put(key, checkpoint);
    }

    public ProjectionCheckpoint get(String key) {
        return store.get(key);
    }

    public void remove(String key) {
        store.remove(key);
    }

    public static class ProjectionCheckpoint {
        public final State state;
        public final Map<String, Integer> tracker;

        private ProjectionCheckpoint(State state, Map<String, Integer> tracker) {
            this.state = state;
            this.tracker = tracker;
        }
    }

    @Override
    public void close() {
        store.close();
    }

}
