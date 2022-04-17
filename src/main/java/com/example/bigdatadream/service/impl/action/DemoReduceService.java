package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoReduceService")
public class DemoReduceService extends BaseJob implements IProcessService {
    /**
     * 聚集RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("Reduce");
        List<Integer> data = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rddData = sc.parallelize(data);
        Integer sum = rddData.reduce((n, m) -> n + m);
        System.out.println(sum);
        sc.close();
    }
}
