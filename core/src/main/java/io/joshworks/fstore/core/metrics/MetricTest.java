package io.joshworks.fstore.core.metrics;

public class MetricTest {
    public static void main(String[] args) throws InterruptedException {

        Metrics abc = MetricRegistry.create("domain1.prefix1");
        Metrics def = MetricRegistry.create("domain1.prefix2");
        Metrics bbb = MetricRegistry.create("domain2.prefix123");

        abc.update("aaaa");
        abc.update("bbbb");

        def.update("cccc");
        def.update("dddd");
        def.update("eeee");

        new Thread(() -> {
            while (true) {
                bbb.update("counter");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }
            }
        }).start();


        Thread.sleep(12000000);

    }
}
