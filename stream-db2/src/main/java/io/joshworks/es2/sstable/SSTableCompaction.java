package io.joshworks.es2.sstable;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.MergeHandle;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

class SSTableCompaction implements Compaction<SSTable> {

    //TODO pass compaction parameters (codec per level, block size etc)

    @Override
    public void compact(MergeHandle<SSTable> handle) {
        File dataFile = handle.replacement();
        File indexFile = SSTable.indexFile(dataFile);

        SegmentChannel dataChannel = SegmentChannel.create(dataFile);
        SegmentChannel indexChannel = SegmentChannel.create(indexFile);

        List<LengthPrefixedDataIterator> items = handle.sources().stream()
                .map(s -> new LengthPrefixedDataIterator(s.data, 0))
                .collect(Collectors.toList());

        while (!items.isEmpty()) {
            List<LengthPrefixedDataIterator> segmentIterators = new ArrayList<>();
            //reversed guarantees that the most recent data is kept when duplicate keys are found
            Iterator<LengthPrefixedDataIterator> itit = items.iterator();
            while (itit.hasNext()) {
                LengthPrefixedDataIterator seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            ByteBuffer nextEntry = getNextEntry(segmentIterators);
            if (nextEntry != null && filter(nextEntry)) {
                long pos = dataChannel.append(nextEntry);
            }
        }

    }

    private boolean filter(ByteBuffer nextEntry) {
        return true;
    }

    private ByteBuffer getNextEntry(List<LengthPrefixedDataIterator> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        LengthPrefixedDataIterator prev = null;
        for (LengthPrefixedDataIterator curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            ByteBuffer prevItem = prev.peek();
            ByteBuffer currItem = curr.peek();
            int c = prevItem.compareTo(currItem); //TODO compare stream then versions

            //TODO unpack on any overlapping
            if (c == 0) { //duplicate remove eldest entry
                curr.next();
            }
            if (c > 0) {
                prev = curr;
            }
        }
        if (prev != null) {
            return prev.next();
        }
        return null;
    }
}
