package de.fom.woobin.datamesh.customerretention.sources;

import java.time.Instant;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import de.fom.woobin.datamesh.customerretention.objects.clickstream.CustomerClickStream;

public class CustomerClickStreamGenerator extends RichParallelSourceFunction<CustomerClickStream> {

    private static final long serialVersionUID = 1L;

    private static final int SLEEP = 300;
            
    private volatile boolean running = true;
    
    @Override
    public void run(SourceContext<CustomerClickStream> ctx) throws Exception {
        CustomerClickStreamIterator iterator = new CustomerClickStreamIterator();

        while (running) {
            CustomerClickStream event = iterator.next();
            ctx.collect(event);
            Thread.sleep(SLEEP);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class CustomerClickStreamIterator {

        static final String ID_PREFIX = "qwerasdf";
        static final String[] USERNAMES = {"Joe1", "ddMichael", "123Fom", "leewoobin", "maxmustermann0", "xDatamesh"};

        private Random random;
        
        public CustomerClickStreamIterator() {
            random = new Random();
        }

        public CustomerClickStream next() {
            CustomerClickStream click = new CustomerClickStream();
            int nextIndex = random.nextInt(USERNAMES.length);
            
            click.setUserName(USERNAMES[nextIndex]);
            click.setItemOfPage(ID_PREFIX + randomIndexWithWeight(
                7, 2, 1,4,5
            ));
            click.setTs(Instant.now().toEpochMilli());
            
            return click;
        }

        private String randomIndexWithWeight(int bound, int weight, int ...weightedIndexes) {
            if (System.currentTimeMillis() % weight == 0) {
                return Integer.toString(
                  weightedIndexes[random.nextInt(weightedIndexes.length)]
                );
            }
            return Integer.toString(
              random.nextInt(bound)
            );
        }
    }
}
