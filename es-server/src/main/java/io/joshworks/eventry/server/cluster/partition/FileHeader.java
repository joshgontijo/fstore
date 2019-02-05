package io.joshworks.eventry.server.cluster.partition;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;

public class FileHeader extends Header {

    static final short HEADER_ID = 3500;

    private String fileName;
    private boolean eof;

    public FileHeader() {
        //de-serialization
    }

    public FileHeader(String fileName, boolean eof) {
        this.fileName = fileName;
        this.eof = eof;
    }

    @Override
    public short getMagicId() {
        return HEADER_ID;
    }

    @Override
    public Supplier<? extends Header> create() {
        return FileHeader::new;
    }

    @Override
    public int serializedSize() {
        return Util.size(fileName) + Global.BYTE_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeObject(fileName, out);
        out.writeBoolean(eof);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        fileName = (String) Util.readObject(in);
        eof = in.readBoolean();
    }

    public String fileName() {
        return fileName;
    }

    public boolean eof() {
        return eof;
    }
}
