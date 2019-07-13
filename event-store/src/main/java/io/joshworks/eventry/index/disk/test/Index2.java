package io.joshworks.eventry.index.disk.test;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.log.LogEntry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class Index2 {

    public static void main(String[] args) {
        File directory = new File("S:\\event-store\\lsm-tree");
        LsmTree.Builder<IndexKey, Long> builder = LsmTree.builder(directory, new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .sstableBlockFactory(IndexBlock2.factory());

        LsmTree<IndexKey, Long> lsmTree = builder.open();

        lsmTree.put(new IndexKey(1, 0), 0L);
        lsmTree.put(new IndexKey(1, 1), 1L);
        lsmTree.put(new IndexKey(1, 2), 2L);
        lsmTree.put(new IndexKey(1, 3), 3L);

        lsmTree.flushMemTable(true);

        lsmTree.close();


        lsmTree = builder.open();


        CloseableIterator<LogEntry<IndexKey, Long>> iterator = lsmTree.iterator();
        while(iterator.hasNext()) {
            LogEntry<IndexKey, Long> next = iterator.next();
            System.out.println(next);
        }


    }

}
