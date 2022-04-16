package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoPartitionByService")
public class DemoPartitionByService extends BaseJob implements IProcessService {
    /**
     * partitionBy 根据指定分区的规则对数据进行重分区
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("PartitionByDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,6);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = rddData.map(n -> {
            return new Tuple2<>(n, 1);
        });
        System.out.println(tuple2JavaRDD.collect());
        JavaPairRDD<Integer, Integer> wordPair = rddData.mapToPair(n -> new Tuple2<Integer, Integer>(n, 1));
        JavaPairRDD<Integer, Integer> pairRDD = wordPair.partitionBy(new HashPartitioner(2));
        pairRDD.saveAsTextFile("data1");
        JavaPairRDD<Integer, Integer> newPairRdd = pairRDD.partitionBy(new HashPartitioner(2));
        newPairRdd.saveAsTextFile("data2");
        //如果分区器一样(类型和数量)返回的还是自己  RangePartitioner 用于sortBy
        sc.close();
    }
}
