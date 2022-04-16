package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoLeftOuterJoinService")
public class DemoLeftOuterJoinService extends BaseJob implements IProcessService {
    /**
     * 类似于 SQL 语句的左外连接
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("joinDemo");
        List<Tuple2<Integer,String>> data1 = Arrays.asList(new Tuple2<Integer,String>(1,"a"),new Tuple2<Integer,String>(2,"b"),new Tuple2<Integer,String>(3,"c"));
        List<Tuple2<Integer,String>> data2 = Arrays.asList(new Tuple2<Integer,String>(1,"aa"),new Tuple2<Integer,String>(2,"bb"));
        JavaPairRDD<Integer,String> sjrdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer,String> sjrdd2 = sc.parallelizePairs(data2);
        sjrdd1.leftOuterJoin(sjrdd2).collect().forEach(System.out::println);
        /**
         * (1,(a,Optional[aa]))
         * (2,(b,Optional[bb]))
         * (3,(c,Optional.empty))
         */
        sjrdd1.rightOuterJoin(sjrdd2).collect().forEach(System.out::println);
        /**
         * (1,(Optional[a],aa))
         * (2,(Optional[b],bb))
         */
        sc.close();
    }
}
