package io.joshworks.eventry.projections;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProjectionsExecutor extends ThreadPoolExecutor {

    private final Set<ProjectionTask> running = new HashSet<>();

    public ProjectionsExecutor(int threads) {
        super(threads, threads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        ProjectionTask task = (ProjectionTask) r;
        t.setName("projection-task-" + task.projection.name);
        this.running.add(task);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        ProjectionTask task = (ProjectionTask) r;
        this.running.remove(task);
    }

    public Set<ProjectionTask> runningTasks() {
        return new HashSet<>(running);
    }

}
