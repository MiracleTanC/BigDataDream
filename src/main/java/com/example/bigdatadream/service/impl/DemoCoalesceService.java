package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service("DemoCoalesceService")
public class DemoCoalesceService extends BaseJob implements IProcessService {
    /**
     * 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
     * 当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少分区的个数，减小任务调度成本
     * 默认不会将数据打乱重新组合，可能会导致数据不均衡，数据倾斜
     * 如果想让数据均衡，则可以进行shuffle处理
     * 扩大分区必须shuffle处理
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("CoalesceDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        //指定3个分区
        JavaRDD<Integer> rddData = sc.parallelize(data,3);
        JavaRDD<Integer> newRdd = rddData.coalesce(2);
        newRdd.saveAsTextFile("coalesce");//src同级增加了一个coalesce目录，只有两个分区
        //查看分区数据不均衡，分区00000 [1,2] 00001 [3,4,5,6]
        JavaRDD<Integer> newRdd2 = rddData.coalesce(2,true);
        newRdd2.saveAsTextFile("coalesce2");
        //查看分区数据均衡，分区00000 [1,4,5] 00001 [2,3,6]

        //扩大分区,不进行shuffle,不起作用
        JavaRDD<Integer> newRdd3 = rddData.coalesce(3,true);
        newRdd3.saveAsTextFile("coalesce3");//顺序较乱，考虑使用reparation，底层就是使用coalesce

        JavaRDD<Integer> newRdd4 = newRdd2.repartition(3);
        newRdd4.saveAsTextFile("repartition");
        sc.close();
    }
}
