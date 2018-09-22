package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.projections.ExecutionStatus;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.snappy.http.HttpExchange;
import org.apache.http.HttpStatus;

import java.util.Collection;

public class ProjectionsEndpoint {

    private final EventStore store;

    public ProjectionsEndpoint(EventStore store) {
        this.store = store;
    }

    public void create(HttpExchange exchange) {
        Projection projection = exchange.body().asObject(Projection.class);

        Projection created = store.createProjection(projection.name, projection.script, projection.type, projection.enabled);
        exchange.status(201).send(created);
    }

    public void update(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        Projection projection = exchange.body().asObject(Projection.class);

        store.updateProjection(name, projection.script, projection.type, projection.enabled);
        exchange.status(HttpStatus.SC_NO_CONTENT).end();
    }

    public void runAdHocQuery(HttpExchange exchange) {

    }

    public void run(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        store.runProjection(name);
    }

    public void executionStatus(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        ExecutionStatus executionStatus = store.projectionExecutionStatus(name);
        if(executionStatus == null) {
            exchange.status(404);
            return;
        }
        exchange.send(executionStatus);
    }

    public void executionStatuses(HttpExchange exchange) {
        Collection<ExecutionStatus> executionStatus = store.projectionExecutionStatuses();
        exchange.send(executionStatus);
    }

    public void getAll(HttpExchange exchange) {
        exchange.send(store.projections());
    }

    public void delete(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        store.deleteProjection(name);
    }

    //TODO improve exception handling for all CRUD operations
    public void get(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        exchange.send(store.projection(name));
    }


}
