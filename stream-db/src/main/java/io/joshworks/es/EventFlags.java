//package io.joshworks.es;
//
//
//public class EventFlags {
//
//    public static final int DELETION_ATTR = 0;
//    private static final int HAS_MAX_AGE = 1 << 1;
//
//    public static boolean deletion(Record record) {
//        return record.hasAttribute(DELETION_ATTR);
//    }
//
//    public static boolean expired(Record record, long maxAgeSeconds) {
//        boolean hasMaxAge = record.hasAttribute(HAS_MAX_AGE);
//        if (!hasMaxAge) {
//            return false;
//        }
//        long timestamp = record.timestamp();
//        long now = System.currentTimeMillis();
//        long diffSec = (now - timestamp) / 1000;
//        return maxAgeSeconds > 0 && (diffSec > maxAgeSeconds);
//    }
//}
