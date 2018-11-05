package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.fstore.core.io.IOUtils;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.joshworks.eventry.utils.StringUtils.requireNonBlank;

public class Projections implements Closeable {

    public static final String ENGINE_NAME = "nashorn";
    private final ProjectionExecutorManager manager;
    private final Map<String, Projection> projectionsMap = new HashMap<>();

    public static final String PROJECTIONS_RESOURCE_FOLDER = "projections"; //TODO this should not be hardcoded
    private static final Set<String> systemProjectionFiles = Set.of("by-type.js");

    private final Set<String> systemProjectionNames;

    public Projections(ProjectionExecutorManager manager) {
        this.manager = manager;
        this.systemProjectionNames = getSystemProjections().stream().map(p -> p.name).collect(Collectors.toSet());
    }

    public void createSystemProjections() {
        List<Projection> projections = getSystemProjections();
        for (Projection projection : projections) {
            projectionsMap.put(projection.name, projection);
        }
    }

    private List<Projection> getSystemProjections() {
        List<Projection> found = new ArrayList<>();
        for (String scriptFile : systemProjectionFiles) {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROJECTIONS_RESOURCE_FOLDER + "/" + scriptFile);
            if (is == null) {
                throw new ProjectionException("Failed to load script file " + scriptFile + ": file not found");
            }
            String script = IOUtils.toString(is);
            Projection projection = Jsr223Handler.compile(script, ENGINE_NAME);
            found.add(projection);
        }
        return found;
    }

    public void add(Projection projection) {
        projectionsMap.put(projection.name, projection);
    }

    //TODO provide engine name from the content type or header
    public Projection create(String script) {
        requireNonBlank(script, "script");

        Projection projection = Jsr223Handler.compile(script, ENGINE_NAME);
        if (projectionsMap.containsKey(projection.name)) {
            throw new IllegalArgumentException("Projection with name '" + projection.name + "' already exist");
        }

        projectionsMap.put(projection.name, projection);
        return projection;
    }

    public void runAdHoc(String script, IEventStore store) {
        throw new UnsupportedOperationException("TODO - implement me");
    }

    public void run(String name, IEventStore store) {
        Projection projection = get(name);
        if (!projection.enabled) {
            throw new RuntimeException("Projection is not enabled");
        }
        manager.run(projection, store);
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

    public Map<String, Metrics> executionStatus(String name) {
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
        if(systemProjectionNames.contains(name)) {
            throw new IllegalArgumentException("Cannot update system projection");
        }

        projectionsMap.remove(name);
        Projection updated = Jsr223Handler.compile(script, ENGINE_NAME);
        projectionsMap.put(updated.name, updated);

        return updated;
    }

    public void delete(String name) {
        requireNonBlank(name, "name");

        if(systemProjectionNames.contains(name)) {
            throw new IllegalArgumentException("Cannot update system projection");
        }

        Projection projection = projectionsMap.remove(name);
        if (projection == null) {
            throw new IllegalArgumentException("No projection found for id " + name);
        }
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
    }
}
