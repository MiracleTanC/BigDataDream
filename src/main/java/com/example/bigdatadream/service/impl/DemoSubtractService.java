package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoSubtractService")
public class DemoSubtractService extends BaseJob implements IProcessService {
    /**
     * 以一个 RDD 元素为主，去除两个 RDD 中重复元素，将其他元素保留下来。求差集
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("subtractDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,6);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> data2 = Arrays.asList(5, 6, 7, 8, 9);
        JavaRDD<Integer> rddData2 = sc.parallelize(data2);
        JavaRDD<Integer> javaRdd = rddData.subtract(rddData2);//差集  [1,2,3,4]
        System.out.println(javaRdd.collect());

        sc.close();
    }
}
