import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Order implements Runnable{
    int thread_id;
    BufferedReader br;
    ExecutorService tpe;
    AtomicInteger inQueue;
    String order_products_fname;
    BufferedWriter orders_bw;
    BufferedWriter products_bw;
    public Order(int thread_id, BufferedReader br, ExecutorService tpe,
                 String order_products_fname,
                 BufferedWriter orders_bw,
                 BufferedWriter products_bw,
                 AtomicInteger inQueue) {
        this.thread_id = thread_id;
        this.br = br;
        this.tpe = tpe;
        this.order_products_fname = order_products_fname;
        this.orders_bw = orders_bw;
        this.products_bw = products_bw;
        this.inQueue = inQueue;
    }

    @Override
    public void run() {
        String order_name;
        String line;
        int nr_order_products;

        while (true) {
            synchronized (this) {
                try {
                    if ((line = br.readLine()) == null) break;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            String[] line_values = line.split(",");
            order_name = line_values[0];
            nr_order_products = Integer.parseInt(line_values[1]);
            if (nr_order_products == 0)
                continue;

            List<Boolean> products_shipped = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(nr_order_products, Boolean.FALSE)));
            Future<?>[] tasks = new Future[nr_order_products];
            for (int i = 0; i < nr_order_products; i++) {
                inQueue.incrementAndGet();
                tasks[i] = tpe.submit(new Product(order_products_fname, order_name, i, products_shipped, orders_bw, products_bw, inQueue, tpe));
            }
            for (int i = 0; i < nr_order_products; i++) {
                try {
                    tasks[i].get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            int all_shipped = 1;
            for (Boolean prod:products_shipped) {
                if (prod == Boolean.FALSE) {
                    all_shipped = 0;
                    break;
                }
            }
            if (all_shipped == 1) {
                try {
                    orders_bw.write(order_name + "," + nr_order_products + ",shipped" + "\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        int left = inQueue.get();
        if (left == 0) {
            tpe.shutdown();
        }
    }
}
