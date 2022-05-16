package io.joshworks.es2.log;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.directory.SegmentId;
import io.joshworks.es2.directory.View;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.joshworks.fstore.core.iterators.Iterators.stream;

public class TLogIterator implements CloseableIterator<ByteBuffer> {

    private final SegmentDirectory<SegmentChannel> directory;
    private View<SegmentChannel> currentView;
    private CloseableIterator<ByteBuffer> segmentIterator;


    public TLogIterator(SegmentDirectory<SegmentChannel> directory) {
        this.directory = directory;
        this.currentView = directory.view();
        this.segmentIterator = iterator(currentView, null);
    }

    private CloseableIterator<ByteBuffer> iterator(View<SegmentChannel> view, SegmentId lastSegment) {
        var iterators = stream(view.reverse())
                .filter(sc -> lastSegment == null || lastSegment.compareTo(sc.segmentId()) > 0)
                .map(LengthPrefixedChannelIterator::new)
                .toList();

        return Iterators.concat(iterators);
    }

    @Override
    public boolean hasNext() {
        if (segmentIterator.hasNext()) {
            return true;
        }
        if (currentView.generation() == directory.generation()) { //no changes
            return false;
        }
        //view has changed and current iterator has no elements, time to go to the next view
        var lastSegment = this.currentView.tail().segmentId();
        this.currentView.close();
        this.currentView = directory.view();
        this.segmentIterator = iterator(this.currentView, lastSegment);
        return segmentIterator.hasNext();
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return segmentIterator.next();
    }

    @Override
    public void close() {
        segmentIterator.close();
        currentView.close();
    }
}
