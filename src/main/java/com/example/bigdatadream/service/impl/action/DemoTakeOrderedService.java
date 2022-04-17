package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoTakeOrderedService")
public class DemoTakeOrderedService extends BaseJob implements IProcessService {
    /**
     * 返回该 RDD 排序后的前 n 个元素组成的数组
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("TakeOrdered");
        List<Integer> data = Arrays.asList(1, 6, 3, 2,5);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        List<Integer> take = rddData.takeOrdered(3);//默认升序
        System.out.println(take);
        sc.close();
    }
}
