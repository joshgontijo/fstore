package io.joshworks.es2.log;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.directory.SegmentId;
import io.joshworks.es2.directory.View;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class TLogIterator implements CloseableIterator<ByteBuffer> {

    private final SegmentDirectory<SegmentChannel> directory;
    private View<SegmentChannel> currentView;
    private Iterator<SegmentChannel> viewIterator;
    private CloseableIterator<ByteBuffer> segmentIterator;
    private SegmentId lastSegment;


    public TLogIterator(SegmentDirectory<SegmentChannel> directory) {
        this.directory = directory;
        this.currentView = directory.view();
        this.viewIterator = currentView.iterator();
        this.segmentIterator = new LengthPrefixedChannelIterator(this.viewIterator.next());
    }

    private void nextIterator() {
        if (viewIterator.hasNext()) {
            return;
        }
        View<SegmentChannel> newView = directory.view();
        if (currentView.generation() == newView.generation()) {
            newView.close();
            this.segmentIterator = Iterators.empty();
            return;
        }
        currentView.close();
        currentView = newView;
        viewIterator = currentView.iterator();

        //TODO compare with lastSegmentId in order to read only new segment
        this.segmentIterator = new LengthPrefixedChannelIterator(this.viewIterator.next());
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ByteBuffer next() {
        return null;
    }

    @Override
    public void close() {
        segmentIterator.close();
        currentView.close();
    }
}
