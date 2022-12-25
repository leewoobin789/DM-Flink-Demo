package de.fom.woobin.datamesh.inventory.sources;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import de.fom.woobin.datamesh.inventory.objects.stock.ItemStock;

public class ItemStockGenerator extends RichParallelSourceFunction<ItemStock> {

    private static final long serialVersionUID = 1L;

    private static final int SLEEP = 10000;
            
    private volatile boolean running = true;
    
    @Override
    public void run(SourceContext<ItemStock> ctx) throws Exception {
        ItemStockIterator iterator = new ItemStockIterator();

        while (running) {
            ItemStock event = iterator.next();
            ctx.collect(event);
            Thread.sleep(SLEEP);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class ItemStockIterator {

        static final String ID_PREFIX = "qwerasdf";
        static final String[] ITEMS = {"Socks", "Jacket", "Trouser", "Underwear", "Shoes", "Hat", "Tshirt"};

        private Random random;
        private DateTimeFormatter formatter;

        public ItemStockIterator() {
            random = new Random();
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }

        public ItemStock next() {
            ItemStock itemStock = new ItemStock();
            int nextIndex = random.nextInt(ITEMS.length);
            itemStock.setId(ID_PREFIX + Integer.toString(nextIndex));
            itemStock.setItemName(ITEMS[nextIndex]);
            itemStock.setUpdatedTs(formatter.format(LocalDateTime.now()));

            int randNumber =random.nextInt(100);
            if (randNumber%2 == 0) {
                itemStock.setStock(1000 + randNumber);
            } else {
                itemStock.setStock(1000 - randNumber);
            }
            
            return itemStock;
        }
    }
}
