package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoAggregateService")
public class DemoAggregateService extends BaseJob implements IProcessService {
    /**
     * 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("Aggregate");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data,3);
        //三个参数 （初始值，分区内的比较规则，分区间的比较规则）
        Integer aggregate = rddData.aggregate(0, (n, m) -> n + m, (n1, m1) -> n1 + m1);
        System.out.println(aggregate);
        sc.close();
    }
}
