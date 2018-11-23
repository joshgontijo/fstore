package io.joshworks.eventry.projections.result;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExecutionResultTest {

    @Test
    public void failed_when_at_least_one_item_has_failed() {
        Status status = ExecutionResult.getStatusFlag(List.of(Status.COMPLETED, Status.FAILED));
        assertEquals(Status.FAILED, status);
    }

    @Test
    public void completed_when_all_items_have_completed() {
        Status status = ExecutionResult.getStatusFlag(List.of(Status.COMPLETED, Status.COMPLETED));
        assertEquals(Status.COMPLETED, status);
    }

    @Test
    public void running_when_at_least_on_item_is_running_and_no_failed_tasks() {
        Status status = ExecutionResult.getStatusFlag(List.of(Status.COMPLETED, Status.RUNNING));
        assertEquals(Status.RUNNING, status);
    }

    @Test
    public void returns_running_when_at_least_one_task_is_running() {
        Status status = ExecutionResult.getStatusFlag(List.of(Status.FAILED, Status.RUNNING));
        assertEquals(Status.RUNNING, status);
    }

    @Test
    public void running_when_at_least_on_item_is_running_and_no_awaiting_tasks() {
        Status status = ExecutionResult.getStatusFlag(List.of(Status.AWAITING, Status.RUNNING));
        assertEquals(Status.RUNNING, status);
    }
}