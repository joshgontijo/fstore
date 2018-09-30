package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.meta.Metrics;
import io.joshworks.eventry.projections.meta.ProjectionManager;
import io.joshworks.eventry.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Projections {

    private static final Map<String, Projection> items = new HashMap<>();
    private final ProjectionManager manager;


    public Projections(ProjectionManager manager) {
        this.manager = manager;
    }

    public void add(Projection projection) {
        items.put(projection.name, projection);
    }

    public Projection create(String name, Set<String> streams, String script, Projection.Type type, boolean enabled) {
        StringUtils.requireNonBlank(name, "name");
        StringUtils.requireNonBlank(script, "script");
        Objects.requireNonNull(type, "Type must be provided");

        name = name.trim().replaceAll("\\s+", "");

        Projection projection = new Projection(streams, script, name, type, enabled);
        if (items.containsKey(name)) {
            throw new IllegalArgumentException("Projection with name '" + name + "' already exist");
        }

        items.put(name, projection);
        return projection;
    }

    public void runAdHoc(String script, IEventStore store) {
        //TODO
    }

    public void run(String name, IEventStore store) {
        Projection projection = get(name);
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

    public Metrics executionStatus(String name) {
        return manager.status(name);
    }

    public Projection update(String name, String script, Projection.Type type, Boolean enabled) {
        Projection projection = get(name);

        boolean isEnabled = enabled == null ? projection.enabled : enabled;
        Projection.Type newType = type == null ? projection.type : type;
        String newScript = StringUtils.isBlank(script) ? projection.script : script;

        Projection updated = new Projection(projection.streams, newScript, projection.name, newType, isEnabled);
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
