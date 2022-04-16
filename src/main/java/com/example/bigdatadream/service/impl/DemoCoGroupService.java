package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoCoGroupService")
public class DemoCoGroupService extends BaseJob implements IProcessService {
    /**
     * 分组连接
     * 在类型为(K,V)和(K,W)的RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("coGroupDemo");
        List<Tuple2<Integer,String>> data1 = Arrays.asList(new Tuple2<Integer,String>(1,"a"),new Tuple2<Integer,String>(2,"b"),new Tuple2<Integer,String>(3,"c"));
        List<Tuple2<Integer,String>> data2 = Arrays.asList(new Tuple2<Integer,String>(1,"aa"),new Tuple2<Integer,String>(2,"bb"));
        JavaPairRDD<Integer,String> sjrdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer,String> sjrdd2 = sc.parallelizePairs(data2);
        sjrdd1.cogroup(sjrdd2).collect().forEach(System.out::println);
        /**
         * (1,([a],[aa]))
         * (2,([b],[bb]))
         * (3,([c],[]))
         */
        List<Tuple2<Integer,String>> data3 = Arrays.asList(new Tuple2<Integer,String>(1,"a"),new Tuple2<Integer,String>(2,"b"));
        List<Tuple2<Integer,String>> data4 = Arrays.asList(new Tuple2<Integer,String>(1,"aa"),new Tuple2<Integer,String>(2,"bb"),new Tuple2<Integer,String>(2,"bbb"),new Tuple2<Integer,String>(3,"c"));
        JavaPairRDD<Integer,String> sjrdd3 = sc.parallelizePairs(data3);
        JavaPairRDD<Integer,String> sjrdd4 = sc.parallelizePairs(data4);
        sjrdd3.cogroup(sjrdd4).collect().forEach(System.out::println);
        /**
         * (1,([a],[aa]))
         * (2,([b],[bb, bbb]))
         * (3,([],[c]))
         */
        sc.close();
    }
}
