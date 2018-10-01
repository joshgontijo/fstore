package io.joshworks.eventry.projections.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        int status = 0;
        for (TaskResult task : tasks) {
            status = status & task.status.flag;
        }
        if((status & Status.FAILED.flag) == Status.FAILED.flag)  {
            return Status.FAILED;
        }
        if((status & Status.RUNNING.flag) == Status.RUNNING.flag)  {
            return Status.RUNNING;
        }
        if((status & Status.STOPPED.flag) == Status.STOPPED.flag)  {
            return Status.STOPPED;
        }
        if((status & Status.COMPLETED.flag) == Status.COMPLETED.flag)  {
            return Status.COMPLETED;
        }

        throw new RuntimeException("Unknown status");

    }


}
