package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoZipService")
public class DemoZipService extends BaseJob implements IProcessService {
    /**
     * 将两个 RDD 中的元素，以键值对的形式进行合并。其中，键值对中的Key 为第 1 个 RDD
     * 中的元素，Value 为第 2 个 RDD 中的相同位置的元素。
     * 要求分区数量一致,并且每个分区的元素数量要求一致
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("zipDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> data2 = Arrays.asList( 6, 7, 8, 9,10);
        JavaRDD<Integer> rddData2 = sc.parallelize(data2);
        JavaPairRDD<Integer, Integer> zipData = rddData.zip(rddData2);
        System.out.println(zipData.collect());//[(1,6), (2,7), (3,8), (4,9), (5,10)]
        //分区不一致怎么办
//        JavaRDD<Integer> rddData3 = sc.parallelize(data,2);
//        JavaRDD<Integer> rddData4 = sc.parallelize(data2,4);
//        JavaPairRDD<Integer, Integer> zipData2 = rddData3.zip(rddData4);//报错 Can't zip RDDs with unequal numbers of partitions: List(2, 4)
//        System.out.println(zipData2.collect());
        //分区元素不一致怎么办
//        List<Integer> data5 = Arrays.asList( 6, 7, 8, 9,10,11,12);
//        JavaRDD<Integer> rddData5 = sc.parallelize(data5,2);
//        JavaPairRDD<Integer, Integer> zipData3 = rddData3.zip(rddData5);//报错 Can only zip RDDs with same number of elements in each partition
//        System.out.println(zipData3.collect());
        sc.close();
    }
}
