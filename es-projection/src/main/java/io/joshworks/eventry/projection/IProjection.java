package io.joshworks.fstore.projection;


import io.joshworks.fstore.projection.result.Metrics;
import io.joshworks.fstore.projection.result.TaskStatus;

import java.util.Collection;
import java.util.Map;

public interface IProjection {

    Collection<Projection> projections();

    Projection projection(String name);

    Projection createProjection(String script);

    Projection updateProjection(String name, String script);

    void deleteProjection(String name);

    void runProjection(String name);

    void resetProjection(String name);

    void stopProjectionExecution(String name);

    void disableProjection(String name);

    void enableProjection(String name);

    Map<String, TaskStatus> projectionExecutionStatus(String name);

    Collection<Metrics> projectionExecutionStatuses();

}
