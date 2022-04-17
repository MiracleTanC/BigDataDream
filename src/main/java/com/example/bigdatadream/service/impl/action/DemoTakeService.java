package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoTakeService")
public class DemoTakeService extends BaseJob implements IProcessService {
    /**
     * 返回一个由RDD 的前 n 个元素组成的数组
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("take");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> take = rddData.take(3);
        System.out.println(take);
        sc.close();
    }
}
