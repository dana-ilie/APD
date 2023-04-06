import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema2 {
    public static void main(String[] args) {
        String folder_input = args[0];
        int nr_max_threads = Integer.parseInt(args[1]);

        // output orders
        File orders_out_file = new File("orders_out.txt");
        if (!orders_out_file.exists()) {
            try {
                orders_out_file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        FileWriter orders_fw;
        try {
            orders_fw = new FileWriter(orders_out_file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BufferedWriter orders_bw = new BufferedWriter(orders_fw);

        // output products
        File products_out_file = new File("order_products_out.txt");
        if (!products_out_file.exists()) {
            try {
                products_out_file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        FileWriter products_fw;
        try {
            products_fw = new FileWriter(products_out_file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BufferedWriter products_bw = new BufferedWriter(products_fw);

        // input
        String products_file_name = folder_input + "/order_products.txt";

        String orders_file_name = folder_input + "/orders.txt";
        File orders_file = new File(orders_file_name);
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(orders_file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        AtomicInteger inQueue = new AtomicInteger(0);
        ExecutorService tpe = Executors.newFixedThreadPool(nr_max_threads);

        // start P level 1 threads
        Thread[] order_threads = new Thread[nr_max_threads];
        for (int i = 0; i < nr_max_threads; i++) {
            order_threads[i] = new Thread(new Order(i, br, tpe, products_file_name, orders_bw, products_bw, inQueue));
            order_threads[i].start();
        }

        // join P threads
        for (int i = 0; i < nr_max_threads; i++) {
            try {
                order_threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // close files
        try {
            orders_bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            products_bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
