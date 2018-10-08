package io.joshworks.eventry;

import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.result.Metrics;

import java.util.Collection;
import java.util.Map;

public interface IProjection {


    Collection<Projection> projections();

    Projection projection(String name);

    Projection createProjection(String script);

    Projection updateProjection(String name, String script);

    void deleteProjection(String name);

    void runProjection(String name);

    Map<String, Metrics> projectionExecutionStatus(String name);

    Collection<Metrics> projectionExecutionStatuses();

}
