package io.joshworks.eventry;

import io.joshworks.eventry.projections.ExecutionStatus;
import io.joshworks.eventry.projections.Projection;

import java.util.Collection;

public interface IProjection {


    Collection<Projection> projections();

    Projection projection(String name);

    Projection createProjection(String name, String script, Projection.Type type, boolean enabled);

    Projection updateProjection(String name, String script, Projection.Type type, Boolean enabled);

    void deleteProjection(String name);

    void runProjection(String name);

    ExecutionStatus projectionExecutionStatus(String name);

    Collection<ExecutionStatus> projectionExecutionStatuses();

}
