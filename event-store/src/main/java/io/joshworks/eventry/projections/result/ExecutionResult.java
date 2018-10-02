package io.joshworks.eventry.projections.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutionResult {

    public final String projectionName;
    public final Map<String, Object> options;
    public final List<TaskResult> tasks = new ArrayList<>();

    public ExecutionResult(String projectionName, Map<String, Object> options) {
        this.projectionName = projectionName;
        this.options = options;
    }

    public ExecutionResult addTask(TaskResult taskResult) {
        this.tasks.add(taskResult);
        return this;
    }

    public ExecutionResult addTasks(List<TaskResult> taskResults) {
        this.tasks.addAll(taskResults);
        return this;
    }

    public Status getOverallStatus() {
        return getStatusFlag(tasks.stream().map(task -> task.status).collect(Collectors.toList()));

    }

    Status getStatusFlag(List<Status> statuses) {
        int statusFlag = 0;
        for (Status status : statuses) {
            statusFlag = statusFlag | status.flag;
        }
        if((statusFlag & Status.RUNNING.flag) == Status.RUNNING.flag)  {
            return Status.RUNNING;
        }
        if((statusFlag & Status.FAILED.flag) == Status.FAILED.flag)  {
            return Status.FAILED;
        }
        if((statusFlag & Status.STOPPED.flag) == Status.STOPPED.flag)  {
            return Status.STOPPED;
        }
        if((statusFlag & Status.COMPLETED.flag) == Status.COMPLETED.flag)  {
            return Status.COMPLETED;
        }

        throw new RuntimeException("Unknown statusFlag");

    }


}
