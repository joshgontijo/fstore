package io.joshworks.es2.sstable;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.MergeHandle;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;
import io.joshworks.fstore.core.iterators.PeekingIterator;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

class SSTableCompaction implements Compaction<SSTable> {

    //TODO pass compaction parameters (codec per level, block size etc)

//    @Override
//    public void compact(MergeHandle<SSTable> handle) {
//        List<PeekingIterator<ByteBuffer>> iterators = handle.sources()
//                .stream()
//                .map(s -> s.channel)
//                .map(LengthPrefixedChannelIterator::new)
//                .map(Iterators::closeableIterator)
//                .map(Iterators::peekingIterator)
//                .collect(Collectors.toList());
//
//        //TODO if recompaction is required then:
//        //- decompress blocks (streaming entries from blocks), must be added before mergeSort iterator
//        //- construct a new SStable using the new compression
//        //- use SSTable.create to append each entry
//        CloseableIterator<ByteBuffer> merging = Iterators.merging(iterators, StreamBlock::compare);
//        SSTable.writeBlocks(handle.replacement(), merging);
//    }

    @Override
    public void compact(MergeHandle<SSTable> handle) {
        List<PeekingIterator<ByteBuffer>> iterators = handle.sources()
                .stream()
                .map(s -> s.channel)
                .map(LengthPrefixedChannelIterator::new)
                .map(StreamBlockIterator::new)
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());

        //TODO if recompaction is required then:
        //- decompress blocks (streaming entries from blocks), must be added before mergeSort iterator
        //- construct a new SStable using the new compression
        //- use SSTable.create to append each entry
        CloseableIterator<ByteBuffer> merging = Iterators.merging(iterators, StreamBlock::compare);
        //TODO move to constructor or MergeHandle parameter
        SSTable.create(handle.replacement(), merging, BlockCodec.SNAPPY, Memory.PAGE_SIZE)
                .close();
    }
}

