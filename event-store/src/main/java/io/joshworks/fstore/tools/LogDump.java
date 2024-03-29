package io.joshworks.fstore.tools;

import io.joshworks.fstore.EventLogIterator;
import io.joshworks.fstore.EventStore;
import io.joshworks.fstore.LinkToPolicy;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.SystemEventPolicy;
import io.joshworks.fstore.api.IEventStore;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.LogIterator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogDump {

    public static void main(String[] args) {
        IEventStore store = EventStore.open(new File("J:\\event-store-github"));
        dumpLog(new File("J:\\event-store-github\\log-dump.log"), store);
//        dumpIndex(new File("J:\\event-store\\idx-dump.log"), store);
    }

    public static void dump(File file, EventLogIterator iterator) {
        try (var fileWriter = new FileWriter(file)) {
            while (iterator.hasNext()) {
                EventRecord event = iterator.next();
                fileWriter.write(event.toString() + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dumpStream(String stream, File file, IEventStore store) {
        try (var fileWriter = new FileWriter(file); var iterator = store.fromStream(EventId.parse(stream))) {
            while (iterator.hasNext()) {
                EventRecord event = iterator.next();
                fileWriter.write(event.toString() + System.lineSeparator());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dumpLog(File file, IEventStore store) {
        try (var fileWriter = new FileWriter(file); var iterator = store.fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.INCLUDE)) {
            while (iterator.hasNext()) {
                EventRecord event = iterator.next();
                fileWriter.write(event.toString() + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void dump(File file, CloseableIterator<T> iterator) {
        try (var fileWriter = new FileWriter(file)) {
            while (iterator.hasNext()) {
                T item = iterator.next();
                fileWriter.write(item + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(iterator);
        }
    }

    public static <T> void dump(File file, LogIterator<T> iterator) {
        try (var fileWriter = new FileWriter(file)) {
            while (iterator.hasNext()) {
                long position = iterator.position();
                T item = iterator.next();
                fileWriter.write(position + " | " + item + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(iterator);
        }
    }

//    public static void dumpIndex(File file, IEventStore store) {
//        System.out.println("Dumping index");
//        try (var fileWriter = new FileWriter(file)) {
//            LogIterator<IndexEntry> iterator = store.scanIndex();
//            while (iterator.hasNext()) {
//                long position = iterator.position();
//                IndexEntry event = iterator.next();
//                fileWriter.write(position + " | " + event.toString() + System.lineSeparator());
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println("Dumping index complete");
//    }


}
