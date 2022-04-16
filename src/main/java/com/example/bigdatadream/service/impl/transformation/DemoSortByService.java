package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.*;

@Service("DemoSortByService")
public class DemoSortByService extends BaseJob implements IProcessService {
    /**
     * 该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理的结果进行排序，
     * 默认为升序排列。排序后新产生的 RDD 的分区数与原RDD 的分区数一致。中间存在 shuffle 的过程
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("SortByDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        JavaRDD<Integer> integerJavaRDD = rddData.sortBy(n -> n, false, 1);
        System.out.println(integerJavaRDD.collect());
        //按map的某一列排序
        URL resource = this.getClass().getClassLoader().getResource("data/city_info.txt");
        if (resource != null) {
            String filePath = resource.getFile();
            JavaRDD<String> fileRDD = sc.textFile(filePath);
            JavaRDD<Map<String, String>> mapJavaRDD = fileRDD.flatMap(n -> {
                List<Map<String, String>> ds = new ArrayList<>();
                String[] crr = n.split("\t");
                Map<String, String> mp = new HashMap<>();
                mp.put("id", crr[0]);
                mp.put("city", crr[1]);
                mp.put("area", crr[2]);
                ds.add(mp);
                return ds.iterator();
            }).sortBy(n->Integer.parseInt(n.get("id")),false,1);
            System.out.println(mapJavaRDD.collect());
        }
        sc.close();
    }
}
