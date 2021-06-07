package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class IndexWriter implements Closeable {

    private static final ThreadLocal<TreeBuffers> buffer = ThreadLocal.withInitial(TreeBuffers::new);
    private final SegmentChannel channel;

    public IndexWriter(SegmentChannel channel) {
        this.channel = channel;
    }

    public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
        Block node = buffer.get().getOrAllocate(0);
        if (!node.add(stream, version, recordSize, recordEntries, logPos)) {
            writeNode(node);
            node.add(stream, version, recordSize, recordEntries, logPos);
        }
    }

    public void writeNode(Block node) {
        int idx = node.writeTo(channel);

        //link node to the parent
        Block parent = buffer.get().getOrAllocate(node.level() + 1);
        if (!parent.addLink(node, idx)) {
            writeNode(parent);
            parent.addLink(node, idx);
        }
    }

    //write not without linking node to the parent, used only for root when completing segment
    private void writeNodeFinal(Block node) {
        node.writeTo(channel);
    }

    public void complete() {
        //flush remaining nodes
        List<Block> nodeBlocks = buffer.get().nodeBlocks;
        for (int i = 0; i < nodeBlocks.size() - 1; i++) {
            Block block = nodeBlocks.get(i);
            if (block.hasData()) {
                writeNode(block);
            }
        }
        //flush root
        Block root = nodeBlocks.get(nodeBlocks.size() - 1);
        if (root.hasData()) {
            writeNodeFinal(root);
        }

        for (Block block : nodeBlocks) {
            assert !block.hasData();
            block.clear();
        }

        channel.truncate();
        channel.flush();

        assert channel.size() % BPTreeIndexSegment.BLOCK_SIZE == 0 : "Unaligned index";
    }

    @Override
    public void close() {
        for (Block block : buffer.get().nodeBlocks) {
            block.clear();
        }
    }


    private static class TreeBuffers {
        private final List<Block> nodeBlocks = new ArrayList<>();

        private Block getOrAllocate(int level) {
            if (level >= nodeBlocks.size()) {
                nodeBlocks.add(level, Block.create(Memory.PAGE_SIZE, level));
            }
            Block block = nodeBlocks.get(level);
            assert block.level() == level;
            return block;
        }

    }

}
