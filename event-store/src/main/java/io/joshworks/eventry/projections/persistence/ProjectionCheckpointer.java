package io.joshworks.eventry.projections.persistence;

import io.joshworks.eventry.projections.State;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.File;
import java.util.Map;

public class ProjectionCheckpointer {

    private static final String PROJECTIONS_DIR = "projection";
    private static final String PROJECTIONS_STORE_NAME = "projection-checkpointer";
    private final LsmTree<String, ProjectionCheckpoint> store;

    public ProjectionCheckpointer(File rootDir) {
        this.store = LsmTree.open(new File(rootDir, PROJECTIONS_DIR), Serializers.STRING, JsonSerializer.of(ProjectionCheckpoint.class), 1000, PROJECTIONS_STORE_NAME);
    }

    public void checkpoint(String projectionName, State state, Map<String, Integer> position) {
        ProjectionCheckpoint checkpoint = new ProjectionCheckpoint(state, position);
        store.put(projectionName, checkpoint);
    }

    private static class ProjectionCheckpoint {
        public final State state;
        public final Map<String, Integer> position;

        private ProjectionCheckpoint(State state, Map<String, Integer> position) {
            this.state = state;
            this.position = position;
        }
    }

}
