package io.joshworks.es2.directory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CompactionTask extends CompletableFuture<List<CompactionStats>> {

    private final List<CompletableFuture<CompactionStats>> items = new ArrayList<>();
    private final CompletableFuture<Void> task;

    public CompactionTask(List<CompletableFuture<CompactionStats>> items) {
        this.task = CompletableFuture.allOf(items.toArray(CompletableFuture[]::new));
        this.items.addAll(items);
    }

    private List<CompactionStats> mapItems() {
        return items.stream().map(CompletableFuture::join).collect(Collectors.toList());
    }


}
