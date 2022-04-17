package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoFirstService")
public class DemoFirstService extends BaseJob implements IProcessService {
    /**
     * 返回RDD 中的第一个元素
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("first");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        Integer first = rddData.first();
        System.out.println(first);
        sc.close();
    }
}
