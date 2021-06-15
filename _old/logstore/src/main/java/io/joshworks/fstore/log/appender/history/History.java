//package io.joshworks.fstore.log.appender.history;
//
//import io.joshworks.fstore.core.io.Storage;
//import io.joshworks.fstore.core.io.StorageMode;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.log.appender.history.data.HistoryItem;
//import io.joshworks.fstore.log.record.IDataStream;
//import io.joshworks.fstore.log.segment.Segment;
//import io.joshworks.fstore.log.segment.WriteMode;
//
//import java.io.File;
//import java.nio.file.Files;
//
//public class History {
//
//    private static final String DIRECTORY_NAME = "history";
//    private final Segment<HistoryItem> segment;
//
//    public History(File root, String magic, IDataStream dataStream) {
//        File directory = new File(root, DIRECTORY_NAME);
//        if (!directory.exists()) {
//            try {
//                Files.createDirectories(directory.toPath());
//            } catch (Exception e) {
//                throw new RuntimeException("Failed to create history directory", e);
//            }
//        }
//
//        //TODO single segment ?
//        Storage storage = Storage.create(directory, StorageMode.RAF, Size.MB.of(25));
//        this.segment = new Segment<>( new HistorySerializer(), dataStream, magic, WriteMode.LOG_HEAD);
//    }
//
//    public void append(HistoryItem item) {
//
//    }
//
//}
