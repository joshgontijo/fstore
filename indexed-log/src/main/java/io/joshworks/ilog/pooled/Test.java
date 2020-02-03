package io.joshworks.ilog.pooled;

public class Test {

    public static void main(String[] args) {

        ObjectPool.create(PooledRecord.class, 256, p -> new PooledRecord(p, 8, 256));


        try(PooledRecord record = ObjectPool.allocate(PooledRecord.class)) {
            record.buffer();
        }



    }

}
