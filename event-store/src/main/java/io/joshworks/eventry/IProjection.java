package io.joshworks.eventry;

import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskStatus;

import java.util.Collection;
import java.util.Map;

public interface IProjection {


    Collection<Projection> projections();

    Projection projection(String name);

    Projection createProjection(String script);

    Projection updateProjection(String name, String script);

    void deleteProjection(String name);

    void runProjection(String name);

    void resumeProjectionExecution(String name);

    void stopProjectionExecution(String name);

    void disableProjection(String name);

    void enableProjection(String name);

    Map<String, TaskStatus> projectionExecutionStatus(String name);

    Collection<Metrics> projectionExecutionStatuses();

}
