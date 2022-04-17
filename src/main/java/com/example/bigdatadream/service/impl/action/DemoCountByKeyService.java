package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service("DemoCountByKeyService")
public class DemoCountByKeyService extends BaseJob implements IProcessService {
    /**
     * 统计每种 key 的个数
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("CountByKey");
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"), new Tuple2<Integer, String>(3, "c"), new Tuple2<Integer, String>(3, "c"));
        JavaPairRDD<Integer, String> rddData = sc.parallelizePairs(data);
        Map<Integer, Long> mp = rddData.countByKey();
        System.out.println(mp);
        Map<Tuple2<Integer, String>, Long> tuple2LongMap = rddData.countByValue();
        System.out.println(tuple2LongMap);
        sc.close();
    }
}
