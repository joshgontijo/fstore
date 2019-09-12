package io.joshworks.fstore.core.metrics;

public class MetricTest {
    public static void main(String[] args) throws InterruptedException {

        Metrics abc = new Metrics();
        Metrics def = new Metrics();
        Metrics bbb = new Metrics();
        MetricRegistry.register("domain1.prefix1", () -> abc);
        MetricRegistry.register("domain1.prefix2", () -> def);
        MetricRegistry.register("domain2.prefix123", () -> bbb);

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
