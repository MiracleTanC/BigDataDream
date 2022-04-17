package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoCollectService")
public class DemoCollectService extends BaseJob implements IProcessService {
    /**
     * 在驱动程序中，以数组Array 的形式返回数据集的所有元素
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("collect");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> collect = rddData.collect();
        System.out.println(collect);
        for (Integer item : collect) {
            System.out.println(item);
        }
        sc.close();
    }
}
