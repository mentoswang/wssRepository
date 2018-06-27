import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataReader implements Runnable {
    private String path; // 文件路径
    private BlockingQueue<DataRow> queue; // 内存缓冲区
    //private static final int SLEEPTIME = 1000;

    public DataReader(BlockingQueue<DataRow> queue, String path) {
        this.queue = queue;
        this.path = path;
    }

    @Override
    public void run() {
        File file = new File(path);
        DataRow row = null;
       //Random r = new Random();

        try {
            //System.out.println("Start reading file: thread " + Thread.currentThread().getId());
            if (file.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String temp;
                int i = 0;
                while ((temp = br.readLine()) != null) {
                    String[] line = temp.split(",");

                    if (line.length != 2) {
                        System.out.println("error!");
                        return;
                    }

                    //Thread.sleep(r.nextInt(SLEEPTIME));
                    row = new DataRow(line[0], line[1]);
                    //System.out.println(row + " 加入队列");
                    if (!queue.offer(row, 5, TimeUnit.SECONDS)) {
                        System.err.println(" 加入队列失败");
                    }
                }
            }
            //System.out.println("Finish reading file: thread " + Thread.currentThread().getId());
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}