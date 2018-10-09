package io.joshworks.fstore.log.record;

public class RecordHeader {

    public static final int LENGTH_SIZE = Integer.BYTES; //length
    public static final int SECONDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads
    public static final int CHECKSUM_SIZE = Integer.BYTES; //crc32
    public static final int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    public static final int HEADER_OVERHEAD = MAIN_HEADER + LENGTH_SIZE; //length + crc32
}
