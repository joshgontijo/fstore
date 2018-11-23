package io.joshworks.eventry.projections.result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ExecutionResult {

    public final String projectionName;
    public final Status status;
    public final List<TaskStatus> tasks = new ArrayList<>();

    public ExecutionResult(String projectionName, Collection<TaskStatus> tasks) {
        this.projectionName = projectionName;
        this.tasks.addAll(tasks);
        this.status = getStatusFlag(tasks.stream().map(task -> task.status).collect(Collectors.toList()));
    }

    static Status getStatusFlag(List<Status> statuses) {
        int statusFlag = 0;
        for (Status status : statuses) {
            statusFlag = statusFlag | status.flag;
        }
        if ((statusFlag & Status.RUNNING.flag) == Status.RUNNING.flag) {
            return Status.RUNNING;
        }
        if ((statusFlag & Status.AWAITING.flag) == Status.AWAITING.flag) {
            return Status.AWAITING;
        }
        if ((statusFlag & Status.FAILED.flag) == Status.FAILED.flag) {
            return Status.FAILED;
        }
        if ((statusFlag & Status.STOPPED.flag) == Status.STOPPED.flag) {
            return Status.STOPPED;
        }
        if ((statusFlag & Status.COMPLETED.flag) == Status.COMPLETED.flag) {
            return Status.COMPLETED;
        }
        if ((statusFlag & Status.NOT_STARTED.flag) == Status.NOT_STARTED.flag) {
            return Status.NOT_STARTED;
        }

        throw new RuntimeException("Unknown status Flag");
    }

    @Override
    public String toString() {
        return "ExecutionResult{" + "projectionName='" + projectionName + '\'' +
                ", status=" + status +
                ", tasks=" + tasks +
                '}';
    }
}
