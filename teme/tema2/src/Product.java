import java.io.*;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Product implements Runnable{
    String order_products;
    int product_id;
    String order_name;
    List<Boolean> products_shipped;
    BufferedWriter orders_bw;
    BufferedWriter products_bw;
    AtomicInteger inQueue;
    ExecutorService tpe;

    public Product(String order_products, String order_name,
                   int product_id, List<Boolean> products_shipped,
                   BufferedWriter orders_bw,
                   BufferedWriter products_bw,
                   AtomicInteger inQueue,
                   ExecutorService tpe) {
        this.order_products = order_products;
        this.order_name = order_name;
        this.product_id = product_id;
        this.products_shipped = products_shipped;
        this.orders_bw = orders_bw;
        this.products_bw = products_bw;
        this.inQueue = inQueue;
        this.tpe = tpe;
    }

    @Override
    public void run() {
        File products_file = new File(order_products);
        BufferedReader products_br;

        String line;
        try {
            products_br = new BufferedReader(new FileReader(products_file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        int prod_nr = 0;
        while (true) {
            try {
                if ((line = products_br.readLine()) == null) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String[] line_values = line.split((","));
            String order = line_values[0];
            String product = line_values[1];

            synchronized (Product.class) {
                if (order.equals(order_name)) {
                    if (products_shipped.get(prod_nr) == Boolean.FALSE) {
                        products_shipped.set(prod_nr, Boolean.TRUE);
                        prod_nr++;
                        try {
                            products_bw.write(order_name + "," + product + ",shipped" + "\n");
                            inQueue.decrementAndGet();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
