package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.segment.header.LogHeader;

public class SegmentSize {

    private final LogHeader logHeader;
    private final long position;

    public SegmentSize(LogHeader logHeader, long position) {
        this.logHeader = logHeader;
        this.position = position;
    }


    public long physicalSize() {
        return logHeader.physicalSize();
    }

    public long logicalSize() {
        return logHeader.logicalSize();
    }

    public long dataSize() {
        return logHeader.dataSize();
    }

    public long actualDataSize() {
        return logHeader.actualDataSize();
    }

    public long uncompressedDataSize() {
        return logHeader.uncompressedDataSize();
    }

    public long headerSize() {
        return logHeader.headerSize();
    }

    public long footerSize() {
        return logHeader.footerSize();
    }

    public long remainingDataSize() {
        if(logHeader.readOnly()) {
            return 0;
        }
        return dataSize() - position;
    }

    public long uncompressedSize() {
        return logHeader.uncompressedSize();
    }
}
