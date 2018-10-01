package io.joshworks.eventry;

import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.result.Metrics;

import java.util.Collection;
import java.util.Set;

public interface IProjection {


    Collection<Projection> projections();

    Projection projection(String name);

    Projection createProjection(String name, Set<String> streams, String script, Projection.Type type, boolean enabled);

    Projection updateProjection(String name, String script, Projection.Type type, Boolean enabled);

    void deleteProjection(String name);

    void runProjection(String name);

    Metrics projectionExecutionStatus(String name);

    Collection<Metrics> projectionExecutionStatuses();

}
