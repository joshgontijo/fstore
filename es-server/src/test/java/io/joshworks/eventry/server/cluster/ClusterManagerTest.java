package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.node.PartitionedStore;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterManagerTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_BUCKETS = 12;
    private ClusterManager node1; //all data should be written and read from this node
    private ClusterManager node2; //do not communicate with this noe other than verification

    private PartitionedStore store1; //all data should be written and read from this node
    private PartitionedStore store2; //do not communicate with this noe other than verification

    private File testFile1;
    private File testFile2;


    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");
        testFile1 = FileUtils.testFolder();
        testFile2 = FileUtils.testFolder();

        node1 = ClusterManager.connect(testFile1, CLUSTER_NAME, NUM_PARTITIONS, NUM_BUCKETS);
        node2 = ClusterManager.connect(testFile2, CLUSTER_NAME, NUM_PARTITIONS, NUM_BUCKETS);

        store1 = node1.store();
        store2 = node2.store();


        List<String> partitions = new ArrayList<>(store1.nodePartitions(store2.nodeId()));

        node1.reassignBuckets(partitions.get(0), Set.of(6, 7, 8));
        node1.reassignBuckets(partitions.get(1), Set.of(9, 10, 11));

    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(node1);
        IOUtils.closeQuietly(node2);
        FileUtils.tryDelete(testFile1);
        FileUtils.tryDelete(testFile2);
    }

    @Test
    public void append_to_another_node_returns_the_correct_event() {
        var streamName = EventId.of("stream-1", 0);
        EventRecord event = EventRecord.create(streamName.name(), "type", Map.of());

        store1.append(event);
        EventRecord found = store1.get(streamName);

        assertEquals(event.stream, found.stream);
        assertEquals(0, found.version);
    }

    @Test
    public void fromStream_of_node_returns_the_correct_event() {
        String stream = "stream-1";
        EventRecord event1 = EventRecord.create(stream, "type", Map.of());
        EventRecord event2 = EventRecord.create(stream, "type", Map.of());

        store1.append(event1);
        store1.append(event2);
        EventStoreIterator it = store1.fromStream(EventId.of(stream));

        assertTrue(it.hasNext());
        EventRecord found = it.next();
        assertEquals(event1.stream, found.stream);
        assertEquals(0, found.version);

        assertTrue(it.hasNext());
        found = it.next();
        assertEquals(event1.stream, found.stream);
        assertEquals(1, found.version);
    }

    @Test
    public void perf() {
        Set<String> p1 = store1.nodePartitions(store1.nodeId());
        for (int i = 0; i < 5000000; i++) {
            String stream = "stream-" + i;
            String p = store1.partitionOf(stream);
            EventRecord event1 = EventRecord.create(stream, "type", Map.of());
            if (p1.contains(p)) {
                store1.append(event1);
            } else {
                store2.append(event1);
            }
            if (i % 50000 == 0) {
                System.out.println("-> " + i);
                System.out.println(Arrays.toString(store1.hits));
                System.out.println(Arrays.toString(store2.hits));
            }
        }
    }
}