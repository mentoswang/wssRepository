import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

public class HashAggregate {

    public static void aggregate(String path) {

/*
一个 csv 文件，有 1kw 行数据，每行由 a, b 两列构成。a, b 都是整数，文件中的数据没有任何顺序。
要求写一段程序完成和这条 SQL 等价的查询：
select a, count(distinct b), avg(distinct b) from t group by a;

要求：
1. 充分利用 CPU 和 MEM 资源，在结果正确的前提下，查询的响应时间越快越好
2. 需要考虑不同的 group 数量对性能的影响
3. 需要考虑不同的输入数据量对性能的影响
4. 思考如何利用 SIMD 加速
*/

        File file = new File(path);
        //HashMap<Integer, HashSet<Integer>> result = new HashMap<>();
        HashMap<Integer, Object> result = new HashMap<>();

        //TreeSet<Integer> bTreeSet;
        ArrayList<Integer> bArrary;
        HashSet<Integer> bHashSet;

        // for a big group
        //int arrayThreshhold = 5; // execution time: 62968ms
        int arrayThreshhold = 10; // execution time: 52830ms
        //int arrayThreshhold = 20; // execution time: 61102ms

        try {
            if (file.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String temp;
                int i = 0;
                while ((temp = br.readLine()) != null) {
                    if (i % 10000 == 0)
                        System.out.println(i);
                    String[] line = temp.split(",");

                    if (line.length != 2) {
                        System.out.println("error!");
                        return;
                    }
                    int a = Integer.valueOf(line[0]);
                    int b = Integer.valueOf(line[1]);

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
                        bArrary = new ArrayList<Integer>();
                        bArrary.add(b);
                        result.put(a, bArrary);
                    }
                    else if(!(result.get(a) instanceof HashSet)) {
                        bArrary = (ArrayList<Integer>) result.get(a);
                        /* Check if bArray contains b can be optimized by SIMD if using int[] */
                        if(!bArrary.contains(b)) {
                            if(bArrary.size() < arrayThreshhold) {
                                bArrary.add(b);
                                result.replace(a, bArrary);
                            }
                            else {
                                bHashSet = new HashSet<Integer>();
                                for (Integer bKey : bArrary) {
                                    bHashSet.add(bKey);
                                }
                                bHashSet.add(b);
                                result.replace(a, bHashSet);
                            }
                        }
                    }
                    else {
                        bHashSet = (HashSet)result.get(a);
                        if (!bHashSet.contains(b)) {
                            bHashSet.add(b);
                            result.replace(a, bHashSet);
                        }
                    }
                    i++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("a\tcdb\tadb");
        for (int aKey : result.keySet()) {
            int bSum = 0;
            Object bKeys = result.get(aKey);
            if(bKeys instanceof HashSet) {
                for (Integer bKey : (HashSet<Integer>)bKeys) {
                    /* Calculate sum(b) can be optimized by SIMD */
                    bSum += bKey;
                }
                int bCount = ((HashSet<Integer>)bKeys).size();
                /* Calculate avg(b) can be optimized by SIMD */
                double bAvg = (double) bSum / bCount;
                //System.out.println(aKey + "\t" + bAvg + "\t" + bCount);
            }
            else {
                for (Integer bKey : (ArrayList<Integer>)bKeys) {
                    /* Calculate sum(b) can be optimized by SIMD */
                    bSum += bKey;
                }
                int bCount = ((ArrayList<Integer>)bKeys).size();
                 /* Calculate avg(b) can be optimized by SIMD */
                double bAvg = (double) bSum / bCount;
                //System.out.println(aKey + "\t" + bAvg + "\t" + bCount);
            }
        }

        return;
    }
}