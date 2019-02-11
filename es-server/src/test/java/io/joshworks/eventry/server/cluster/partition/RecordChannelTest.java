package io.joshworks.eventry.server.cluster.partition;

import org.junit.Test;

import java.io.File;

public class RecordChannelTest {

    @Test
    public void relativize_on_windows() {
        File root = new File("J:\\a\\b");
        File child = new File("J:\\a\\b\\c\\d.txt");
        System.out.println(root.toPath().getParent().relativize(child.toPath()).toFile().toString());

    }
}