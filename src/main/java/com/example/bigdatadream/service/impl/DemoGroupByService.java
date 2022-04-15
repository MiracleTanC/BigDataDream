package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("DemoGroupByService")
public class DemoGroupByService extends BaseJob implements IProcessService {
    /**
     * groupBy
     * 将数据根据指定的规则进行分组, 分区默认不变，
     * 但是数据会被打乱重新组合，我们将这样的操作称之为shuffle。
     * 极限情况下，数据可能被分在同一个分区中
     * 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("groupByDemo");
        List<Integer> data = Arrays.asList(1, 1, 3, 3, 3, 3, 1, 8, 1, 8, 11);
        //指定2个分区
        JavaRDD<Integer> rddData = sc.parallelize(data, 2);
        //按余数分组
        JavaPairRDD<Integer, Iterable<Integer>> jpRdd = rddData.groupBy(n -> n % 3);
        System.out.print(jpRdd.collect());//[(0,[3, 3, 3, 3]), (2,[8, 8, 11]), (1,[1, 1, 1, 1])],余0，余2，余1
        List<String> data2 = Arrays.asList("Hello", "hive", "hbase", "Hadoop");
        JavaRDD<String> rddData2 = sc.parallelize(data2, 2);
        //将 List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
        JavaPairRDD<String, Iterable<String>> jpRdd2 = rddData2.groupBy(n -> {
            String[] split = n.split("");
            return split[0];
        });
        System.out.print(jpRdd2.collect());//[(h,[hive, hbase]), (H,[Hello, Hadoop])]
        //从服务器日志数据 apache.log 中获取每个时间段访问量。
        URL resource = this.getClass().getClassLoader().getResource("data/apache.log");
        if (resource != null) {
            String filePath = resource.getFile();
            JavaRDD<String> fileRDD = sc.textFile(filePath);
            JavaRDD<Map<String, Integer>> mapJavaRDD = fileRDD.groupBy(m -> {
                String[] arr = m.split(" ");
                String[] dateArr = arr[3].split(":");
                return dateArr[0];
            }).map(n -> {
                Map<String, Integer> it = new HashMap<>();
                int i = 0;
                while (n._2().iterator().hasNext()){
                    i++;
                }
                it.put(n._1(), i);
                return it;
            });
            //[{18/05/2015=2893}, {17/05/2015=1632}, {19/05/2015=2896}, {20/05/2015=2579}]
            System.out.println(mapJavaRDD.collect());
        }
        sc.close();
    }
}
