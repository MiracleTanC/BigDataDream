package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoAggregateByKeyService")
public class DemoAggregateByKeyService extends BaseJob implements IProcessService {
    /**
     * 将数据根据不同的规则进行分区内计算和分区间计算
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("AggregateByKey");
        List<String> data = Arrays.asList("a", "b", "c", "d", "a","b","c");
        JavaRDD<String> rddData = sc.parallelize(data);
//        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> {
//            return new Tuple2<String, Integer>(n, 1);
//        });
        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> new Tuple2<String, Integer>(n, 1));
        System.out.println(tuple2JavaRDD.collect());
        //（初始值，分区数，分区内计算规则，分区间计算规则）
        JavaPairRDD<String, Integer> newJavaRdd = tuple2JavaRDD.aggregateByKey(0, 3, (n, m) -> {
            return n + m;
            //return Math.max(n,m);
        }, (n, m) -> {
            return n + m;
        });
        System.out.println(newJavaRdd.collect());//[(c,2), (d,1), (a,2), (b,2)]

        sc.close();
    }
}
