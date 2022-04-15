package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("DemoDistinctService")
public class DemoDistinctService extends BaseJob implements IProcessService {
    /**
     * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
     * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("distinctDemo");
        List<Integer> data = Arrays.asList(1, 1, 3, 3, 1, 1, 1, 3, 3, 1);
        //指定3个分区
        JavaRDD<Integer> rddData = sc.parallelize(data);

        JavaRDD<Integer> filterData = rddData.distinct();
        System.out.println(filterData.collect());


        sc.close();
    }
}
