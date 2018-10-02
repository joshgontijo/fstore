package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Projections {

    private static final Map<String, Projection> items = new HashMap<>();
    private final ProjectionManager manager;

    public Projections(ProjectionManager manager) {
        this.manager = manager;
    }

    public void add(Projection projection) {
        items.put(projection.name, projection);
    }

    public Projection create(String script) {
        StringUtils.requireNonBlank(script, "script");

        Projection projection = Jsr223Handler.compile(script, "nashorn");

        if (items.containsKey(projection.name)) {
            throw new IllegalArgumentException("Projection with name '" + projection.name + "' already exist");
        }

        items.put(projection.name, projection);
        return projection;
    }

    public void runAdHoc(String script, IEventStore store) {
        //TODO
    }

    public void run(String name, IEventStore store) {
        Projection projection = get(name);
        if(!projection.enabled) {
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
        return new ArrayList<>(items.values());
    }

    public Collection<Metrics> executionStatuses() {
        throw new UnsupportedOperationException("TODO");
    }

    public Map<String, Metrics> executionStatus(String name) {
        return manager.status(name);
    }

    public Projection update(String name, String script) {
        Projection found = get(name);
        if(found == null) {
            throw new IllegalArgumentException("No projection found for name " + name);
        }

        items.remove(name);
        Projection updated = Jsr223Handler.compile(script, "nashorn");
        items.put(updated.name, updated);

        return updated;
    }

    public void delete(String name) {
        StringUtils.requireNonBlank(name, "name");

        Projection projection = items.remove(name);
        if (projection == null) {
            throw new IllegalArgumentException("No projection found for id " + name);
        }
    }

    public Projection get(String name) {
        StringUtils.requireNonBlank(name, "name");

        Projection projection = items.get(name);
        if (projection == null) {
            throw new IllegalArgumentException("No projection found for name '" + name + "'");
        }
        return projection;
    }

}
