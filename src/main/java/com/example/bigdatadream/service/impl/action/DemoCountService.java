package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoCountService")
public class DemoCountService extends BaseJob implements IProcessService {
    /**
     * 返回RDD 中元素的个数
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("count");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        long count = rddData.count();
        System.out.println(count);
        sc.close();
    }
}
