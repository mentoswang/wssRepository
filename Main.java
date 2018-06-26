import java.io.*;
import java.util.Random;

public class Main {

    public static void main(String[] args) {
        String path = "test.csv";
        long begin = System.currentTimeMillis();
        //makeFile(path);
        long mid = System.currentTimeMillis();
        HashAggregate ha = new HashAggregate();
        ha.aggregate(path);
        long end = System.currentTimeMillis();
        System.out.println(end-mid + "ms");
    }

    public static void makeFile(String path) {
        OutputStream out = null;
        int count = 1000 * 10000; // file lines
        try {
            out = new FileOutputStream(new File(path));
            Random r3 = new Random();
            for (int i = 0; i < count; i++) {

                // function test:
                //int a = i%10;
                //int b = i;

                // performance test:
                int a = r3.nextInt(); // a bunch of groups
                //int a = r3.nextInt()%10000; // a lot of groups
                //int a = r3.nextInt()%100; // a few groups
                //int a = 7; // a big group
                int b = r3.nextInt();

                out.write((Integer.valueOf(a).toString() + "," + Integer.valueOf(b).toString() + "\r\n").getBytes());
            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}