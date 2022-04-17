package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoForeachService")
public class DemoForeachService extends BaseJob implements IProcessService {
    /**
     * 分布式遍历RDD 中的每一个元素，调用指定函数
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("foreach");
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"), new Tuple2<Integer, String>(3, "c"), new Tuple2<Integer, String>(3, "c"));
        JavaPairRDD<Integer, String> rddData = sc.parallelizePairs(data);
        // 收集后打印
        rddData.collect().forEach(n-> System.out.printf("key:%s,value:%s",n._1,n._2));
        // 分布式打印
        rddData.foreach(n->System.out.printf("key:%s,value:%s",n._1,n._2));
        sc.close();
    }
}
