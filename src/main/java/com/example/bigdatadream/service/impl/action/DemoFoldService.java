package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoFoldService")
public class DemoFoldService extends BaseJob implements IProcessService {
    /**
     * 折叠操作，aggregate 的简化版操作
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("fold");
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> rddData = sc.parallelize(data,3);
        //三个参数 （初始值，比较规则）分区内核分区间使用同一个比较规则
        Integer aggregate = rddData.fold(0, (n, m) -> n + m);
        System.out.println(aggregate);
        sc.close();
    }
}
