package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.time.temporal.ValueRange;
import java.util.Arrays;
import java.util.List;

@Service("DemoFoldByKeyService")
public class DemoFoldByKeyService extends BaseJob implements IProcessService {
    /**
     * 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为foldByKey
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("FoldByKey");
        List<String> data = Arrays.asList("a", "b", "c", "d", "a","b","c");
        JavaRDD<String> rddData = sc.parallelize(data);
        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> new Tuple2<String, Integer>(n, 1));
        System.out.println(tuple2JavaRDD.collect());
        //（初始值，分区数，分区内计算规则，分区间计算规则）
//        JavaPairRDD<String, Integer> newJavaRdd = tuple2JavaRDD.aggregateByKey(0, 3, (n, m) -> {
//            return n + m;
//            //return Math.max(n,m);
//        }, (n, m) -> {
//            return n + m;
//        });
        JavaPairRDD<String, Integer> newJavaRdd = tuple2JavaRDD.foldByKey(0, 3, (n, m) -> n + m);
        System.out.println(newJavaRdd.collect());//[(c,2), (d,1), (a,2), (b,2)]

        sc.close();
    }
}
