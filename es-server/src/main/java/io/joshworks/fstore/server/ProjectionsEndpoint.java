//package io.joshworks.eventry.server;
//
//import io.joshworks.eventry.api.IEventStore;
//import io.joshworks.eventry.projections.Projection;
//import io.joshworks.eventry.projection.result.Metrics;
//import io.joshworks.eventry.projection.result.TaskStatus;
//import io.joshworks.snappy.http.HttpExchange;
//import org.apache.http.HttpStatus;
//
//import java.util.Collection;
//import java.util.Map;
//
//public class ProjectionsEndpoint {
//
//    private final IEventStore store;
//
//    private static final String PROJECTION_NAME_PATH_PARAM = "name";
//
//
//    public ProjectionsEndpoint(IEventStore store) {
//        this.store = store;
//    }
//
//    public void create(HttpExchange exchange) {
//        String script = exchange.body().asString();
//
//        Projection created = store.createProjection(script);
//        exchange.status(201).send(created);
//    }
//
//    public void update(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        String script = exchange.body().asString();
//
//        store.updateProjection(name, script);
//        exchange.status(HttpStatus.SC_NO_CONTENT).end();
//    }
//
//    public void runAdHocQuery(HttpExchange exchange) {
//        throw new UnsupportedOperationException("TODO");
//    }
//
//    public void getScript(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        Projection projection = store.projection(name);
//        if (projection == null) {
//            exchange.status(404).end();
//            return;
//        }
//        exchange.send(projection.script);
//    }
//
//    public void run(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.runProjection(name);
//    }
//
//    public void stop(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.stopProjectionExecution(name);
//    }
//
//    public void reset(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.resetProjection(name);
//    }
//
//    public void enable(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.enableProjection(name);
//    }
//
//    public void disable(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.disableProjection(name);
//    }
//
//    public void executionStatus(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        Map<String, TaskStatus> executionStatus = store.projectionExecutionStatus(name);
//        if (executionStatus == null) {
//            exchange.status(404);
//            return;
//        }
//        exchange.send(executionStatus);
//    }
//
//    public void executionStatuses(HttpExchange exchange) {
//        Collection<Metrics> executionStatus = store.projectionExecutionStatuses();
//        exchange.send(executionStatus);
//    }
//
//    public void getAll(HttpExchange exchange) {
//        exchange.send(store.projections());
//    }
//
//    public void delete(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        store.deleteProjection(name);
//    }
//
//    //TODO improve exception handling for all CRUD operations
//    public void get(HttpExchange exchange) {
//        String name = exchange.pathParameter(PROJECTION_NAME_PATH_PARAM);
//        exchange.send(store.projection(name));
//    }
//
//}
