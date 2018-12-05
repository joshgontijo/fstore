package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Logging;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.joshworks.eventry.utils.StringUtils.requireNonBlank;

public class Projections implements Closeable {

    private final Logger logger;

    public static final String ENGINE_NAME = "nashorn";
    private final ProjectionExecutor manager;
    private final Map<String, Projection> projectionsMap = new HashMap<>();

    public static final String PROJECTIONS_RESOURCE_FOLDER = "projections";
    private static final Set<String> systemProjectionFiles = Set.of("by-type.js");

    private final Set<String> systemProjectionNames = new HashSet<>();

    public Projections(ProjectionExecutor manager) {
        this.manager = manager;
        this.logger = Logging.namedLogger("projections", "projections");
    }

    public void bootstrapProjections(IEventStore store, Set<String> running) {
        for (String projectionName : running) {
            logger.info("Resuming projection {}", projectionName);
            Projection projection = projectionsMap.get(projectionName);
            if(projection == null) {
                logger.warn("No projection found for name '{}'", projectionName);
                //TODO emit projection failed event
            }

            run(projectionName, store);
        }

        for (Projection projection : projectionsMap.values()) {
            if (Projection.Type.CONTINUOUS.equals(projection.type) && projection.enabled && !running.contains(projection.name)) {
                run(projection.name, store);
            }
        }
    }

    public List<Projection> loadSystemProjections() {
        List<Projection> found = new ArrayList<>();
        for (String scriptFile : systemProjectionFiles) {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROJECTIONS_RESOURCE_FOLDER + "/" + scriptFile);
            if (is == null) {
                throw new ProjectionException("Failed to load script file " + scriptFile + ": file not found");
            }
            String script = IOUtils.toString(is);
            Projection projection = Jsr223Handler.compile(script, ENGINE_NAME);
            found.add(projection);
            systemProjectionNames.add(projection.name);
        }
        return found;
    }

    public void add(Projection projection) {
        projectionsMap.put(projection.name, projection);
    }

    public Projection create(String script) {
        requireNonBlank(script, "script");

        Projection projection = Jsr223Handler.compile(script, ENGINE_NAME);
        if (projectionsMap.containsKey(projection.name)) {
            throw new IllegalArgumentException("Projection with name '" + projection.name + "' already exist");
        }

        projection.enabled = true;
        projectionsMap.put(projection.name, projection);
        return projection;
    }

    public void run(String name, IEventStore store) {
        //TODO implement RESUME
        Projection projection = get(name);
        if (!projection.enabled) {
            throw new RuntimeException("Projection is not enabled");
        }
        manager.run(projection, store);
    }

    public void reset(String name) {
        //TODO implement checkpoint and re-read state from it
        //TODO return checkpoint info to be appended to the internal stream
    }


    public void stop(String name) {
        manager.stop(name);
    }

    public void stopAll() {
        manager.stopAll();
    }

    public Collection<Projection> all() {
        return new ArrayList<>(projectionsMap.values());
    }

    public Collection<Metrics> executionStatuses() {
        throw new UnsupportedOperationException("TODO");
    }

    public Map<String, TaskStatus> executionStatus(String name) {
        return manager.status(name);
    }

//    public Projection toggleEnabled(String name, boolean enabled) {
//        Projection projection = get(name);
//        projection.enabled = enabled;
//    }

    public Projection update(String name, String script) {
        Projection found = get(name);
        if (found == null) {
            throw new IllegalArgumentException("No projection found for name " + name);
        }
        if (systemProjectionNames.contains(name)) {
            throw new IllegalArgumentException("Cannot update system projection");
        }

        projectionsMap.remove(name);
        Projection updated = Jsr223Handler.compile(script, ENGINE_NAME);
        projectionsMap.put(updated.name, updated);

        return updated;
    }

    public void delete(String name) {
        requireNonBlank(name, "name");

        if (systemProjectionNames.contains(name)) {
            throw new IllegalArgumentException("Cannot update system projection");
        }

        Projection projection = projectionsMap.remove(name);
        if (projection == null) {
            throw new IllegalArgumentException("No projection found for id " + name);
        }

        stop(name);
    }

    public Projection get(String name) {
        requireNonBlank(name, "name");

        Projection projection = projectionsMap.get(name);
        if (projection == null) {
            throw new IllegalArgumentException("No projection found for name '" + name + "'");
        }
        return projection;
    }

    @Override
    public void close() {
        stopAll();
        manager.close();
    }
}
