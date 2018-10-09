package io.joshworks.eventry.server;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.snappy.http.HttpExchange;
import org.apache.http.HttpStatus;

import java.util.Collection;
import java.util.Map;

public class ProjectionsEndpoint {

    private final IEventStore store;

    public ProjectionsEndpoint(IEventStore store) {
        this.store = store;
    }

    public void create(HttpExchange exchange) {
        String script = exchange.body().asString();

        Projection created = store.createProjection(script);
        exchange.status(201).send(created);
    }

    public void update(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        String script = exchange.body().asString();

        store.updateProjection(name, script);
        exchange.status(HttpStatus.SC_NO_CONTENT).end();
    }

    public void runAdHocQuery(HttpExchange exchange) {
        throw new UnsupportedOperationException("TODO");
    }

    public void getScript(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        Projection projection = store.projection(name);
        if(projection == null) {
            exchange.status(404).end();
            return;
        }
        exchange.send(projection.script);
    }

    public void run(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        store.runProjection(name);
    }

    public void executionStatus(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        Map<String, Metrics> executionStatus = store.projectionExecutionStatus(name);
        if(executionStatus == null) {
            exchange.status(404);
            return;
        }
        exchange.send(executionStatus);
    }

    public void executionStatuses(HttpExchange exchange) {
        Collection<Metrics> executionStatus = store.projectionExecutionStatuses();
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
