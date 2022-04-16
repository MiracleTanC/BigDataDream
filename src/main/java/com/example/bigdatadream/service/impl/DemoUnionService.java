package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoUnionService")
public class DemoUnionService extends BaseJob implements IProcessService {
    /**
     * 对源RDD 和参数RDD 求并集后返回一个新的RDD
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("unionDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,6);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> data2 = Arrays.asList(5, 6, 7, 8, 9);
        JavaRDD<Integer> rddData2 = sc.parallelize(data2);
        JavaRDD<Integer> intersection = rddData.union(rddData2);//并集[1, 2, 3, 4, 5,6,7,8,9]
        System.out.println(intersection.collect());

        sc.close();
    }
}
