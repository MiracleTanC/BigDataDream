package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoReduceByKeyService")
public class DemoReduceByKeyService extends BaseJob implements IProcessService {
    /**
     * ReduceByKey  可以将数据按照相同的Key 对Value 进行聚合
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("ReduceByKey");
        List<String> data = Arrays.asList("a", "b", "c", "d", "a","b","c");
        JavaRDD<String> rddData = sc.parallelize(data);
//        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> {
//            return new Tuple2<String, Integer>(n, 1);
//        });
        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> new Tuple2<String, Integer>(n, 1));
        System.out.println(tuple2JavaRDD.collect());
        JavaPairRDD<String, Integer> newJavaRdd = tuple2JavaRDD.reduceByKey((n, m) -> {
            return n + m;
        });
        System.out.println(newJavaRdd.collect());

        sc.close();
    }
}
