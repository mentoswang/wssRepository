import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {

        int[] lineCounts = {1, 10};//, 100, 1000};
        int[] groupCounts = {1000, 100, 10, 1};
        int[] bufferCapacitys = {10, 100, 1000};
        int[] arrayThreshholds = {5, 10, 20};
        boolean[] randomInt = {false, true};

        System.out.println("Test File\t\t\tExecution Time(ms)");
        for (int i = 0; i < lineCounts.length; i++) {
            for (int j = 0; j < groupCounts.length; j++) {
                for (boolean random : randomInt) {
                    String path = "l" + lineCounts[i] + "_g" + groupCounts[j] + "_r" + random + ".csv";
                    makeFile(path.toString(), lineCounts[i] * 10000, groupCounts[j], random);
                    long start = System.currentTimeMillis();
                    hashAggregate(path.toString(), bufferCapacitys[2], arrayThreshholds[1]);
                    long end = System.currentTimeMillis();
                    System.out.println(path.toString() + "\t\t\t" + (end - start));
                }
            }
        }
        return;
    }

    public static void makeFile(String path, int rowCount, int groupCount, boolean random) {
        OutputStream out = null;
        try {
            out = new FileOutputStream(new File(path));
            Random r = new Random();
            int a,b;
            for (int i = 0; i < rowCount; i++) {

                if(!random) {
                    a = i % groupCount;
                    b = i;
                }
                else {
                    a = r.nextInt() / groupCount;
                    b = r.nextInt();
                }

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

    public static void hashAggregate(String path, int bufferCapacity, int arrayThreshhold) {
        int consumerCount = Runtime.getRuntime().availableProcessors();
        BlockingQueue<DataRow> queue = new LinkedBlockingDeque<>(bufferCapacity);
        Map result = Collections.synchronizedMap(new HashMap<Integer, Object>());

        ExecutorService service = Executors.newCachedThreadPool();
        DataReader reader = new DataReader(queue, path);
        Future readerFuture = service.submit(reader);

        HashSet<DataConsumer> builders = new HashSet<>();
        HashSet<Future> builderFutures = new HashSet<>();
        for (int count = 0; count < consumerCount; count++) {
            DataConsumer builder = new DataConsumer(queue, result, arrayThreshhold);
            builders.add(builder);
            builderFutures.add(service.submit(builder));
        }

        try {
            Object f = readerFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (readerFuture.isDone()) {
            for (DataConsumer builder : builders) {
                builder.stop();
            }
        }
        for (Future builderFuture : builderFutures) {
            try {
                Object f = builderFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        processResult(result);
    }

    public static void processResult(Map result) {
        /* sum(b) and avg(b) can be optimized by SIMD */
        //System.out.println("a\tcdb\tadb");
        for (Object aKey : result.keySet()) {
            int bSum = 0;
            Object bKeys = result.get(aKey);
            if(bKeys instanceof HashSet) {
                for (Integer bKey : (HashSet<Integer>)bKeys) {
                    bSum += bKey;
                }
                int bCount = ((HashSet<Integer>)bKeys).size();
                double bAvg = (double) bSum / bCount;
                //System.out.println(aKey + "\t" + bCount + "\t" + bAvg);
            }
            else {
                for (Integer bKey : (ArrayList<Integer>)bKeys) {
                    bSum += bKey;
                }
                int bCount = ((ArrayList<Integer>)bKeys).size();
                double bAvg = (double) bSum / bCount;
                //System.out.println(aKey + "\t" + bCount + "\t" + bAvg);
            }
        }
    }
}
