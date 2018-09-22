package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class Projections {

    private static final Map<String, Projection> items = new HashMap<>();
    //TODO externalize
    private final ProjectionsExecutor executor = new ProjectionsExecutor(10);
    private final Map<String, ExecutionStatus> runningTasks = new HashMap<>();

    public void add(Projection projection) {
        items.put(projection.name, projection);
    }

    public Projection create(String name, String script, Projection.Type type, boolean enabled) {
        StringUtils.requireNonBlank(name, "name");
        StringUtils.requireNonBlank(script, "script");
        Objects.requireNonNull(type, "Type must be provided");

        name = name.trim().replaceAll("\\s+", "");

        Projection projection = new Projection(script, name, type, enabled);
        if (items.containsKey(name)) {
            throw new IllegalArgumentException("Projection with name '" + name + "' already exist");
        }

        items.put(name, projection);
        return projection;
    }

    public void runAdHoc(String script, IEventStore store, Consumer<EventRecord> systemRecordAppender) {
        //TODO
    }

    public void run(String name, IEventStore store, Consumer<EventRecord> systemRecordAppender) {
        Projection projection = get(name);
        ProjectionTask projectionTask = new ProjectionTask(projection, store, systemRecordAppender, runningTasks);
        executor.execute(projectionTask);
    }

    public void stop(String name) {
        Projection projection = get(name);
    }

    public void stopAll() {
        executor.runningTasks().forEach(ProjectionTask::stop);
    }

    public Collection<Projection> all() {
        return new ArrayList<>(items.values());
    }

    public Collection<ExecutionStatus> executionStatuses() {
        return new ArrayList<>(runningTasks.values());
    }

    public ExecutionStatus executionStatus(String name) {
        return runningTasks.get(name);
    }

    public Projection update(String name, String script, Projection.Type type, Boolean enabled) {
        Projection projection = get(name);

        boolean isEnabled = enabled == null ? projection.enabled : enabled;
        Projection.Type newType = type == null ? projection.type : type;
        String newScript = StringUtils.isBlank(script) ? projection.script : script;

        Projection updated = new Projection(newScript, projection.name, newType, isEnabled);
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
