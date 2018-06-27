import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DataConsumer implements Runnable {

    private BlockingQueue<DataRow> queue;
    private Map result;
    private Map m;
    private int arrayThreshhold;
    private volatile boolean done = false;
    //private static final int SLEEPTIME = 1000;

    public DataConsumer(BlockingQueue<DataRow> queue, Map result, int arrayThreshhold) {
        this.queue = queue;
        this.result = result;
        this.arrayThreshhold = arrayThreshhold;
    }

    @Override
    public void run() {
        //System.out.println("Start building HashMap: thread " + Thread.currentThread().getId());
        //Random r = new Random();
        int i = 0;
        try {
            while (true) {
                if(done) {
                    if (queue.isEmpty())
                        break;
                }

                ArrayList<Integer> bArrary;
                HashSet<Integer> bHashSet;
                DataRow row = queue.poll(1, TimeUnit.SECONDS);
                int a,b;
                if (row != null) {
                    a = row.getData()[0];
                    b = row.getData()[1];

                    synchronized(result) {
                        if (!result.containsKey(a)) {
                            //bHashSet = new HashSet<Integer>();
                            //bHashSet.add(b);
                            //result.put(a, bHashSet);

                        /* Using an ArrayList to store a small set of b values,
                         * when we got a bunch of groups or distinct values of a,
                         * but few distinct b values for each a,
                         * to save memory usage
                         * (TreeSet is better for search,
                         * but skill consume too much memory) */
                            if (arrayThreshhold > 0) {
                                bArrary = new ArrayList<Integer>();
                                bArrary.add(b);
                                result.put(a, bArrary);
                            }
                            else {
                                bHashSet = new HashSet<Integer>();
                                bHashSet.add(b);
                                result.put(a, bHashSet);
                            }
                        } else if (!(result.get(a) instanceof HashSet)) {
                            bArrary = (ArrayList<Integer>) result.get(a);
                            /* Check if bArray contains b can be optimized by SIMD */
                            if (!bArrary.contains(b)) {
                                if (bArrary.size() < arrayThreshhold) {
                                    bArrary.add(b);
                                    //result.replace(a, bArrary);
                                } else {
                                    bHashSet = new HashSet<Integer>();
                                    for (Integer bKey : bArrary) {
                                        bHashSet.add(bKey);
                                    }
                                    bHashSet.add(b);
                                    result.replace(a, bHashSet);
                                }
                            }
                        } else {
                            bHashSet = (HashSet) result.get(a);
                            bHashSet.add(b);
                            //result.put(a, bHashSet);
                        }
                    }
                    i++;
                }
            }
            //System.out.println("Thread "+ Thread.currentThread().getId() + " built: " + i);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (ConcurrentModificationException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        this.done = true;
    }
}
