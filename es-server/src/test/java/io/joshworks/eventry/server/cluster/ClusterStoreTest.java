package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class ClusterStoreTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final int NUM_PARTITIONS = 4;
    private ClusterStore node1;
    private ClusterStore node2;
    private File testFile1;
    private File testFile2;

    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");
        testFile1 = FileUtils.testFolder();
        testFile2 = FileUtils.testFolder();
        node1 = ClusterStore.connect(testFile1, CLUSTER_NAME, NUM_PARTITIONS);
        node2 = ClusterStore.connect(testFile2, CLUSTER_NAME, NUM_PARTITIONS);

        node1.assignPartition(0);
        node1.assignPartition(1);
        node2.assignPartition(2);
        node2.assignPartition(3);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(node1);
        IOUtils.closeQuietly(node2);
        FileUtils.tryDelete(testFile1);
        FileUtils.tryDelete(testFile2);
    }

    @Test
    public void test_append_remote() {
        node1.appendInternal(EventRecord.create("10000000", "type", Map.of()), 3);
        node2.
    }

}