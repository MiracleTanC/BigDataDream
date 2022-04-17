package com.example.bigdatadream.service.impl.action;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service("DemoSaveService")
public class DemoSaveService extends BaseJob implements IProcessService {
    /**
     * 将数据保存到不同格式的文件中
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("save");
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"), new Tuple2<Integer, String>(3, "c"), new Tuple2<Integer, String>(3, "c"));
        JavaPairRDD<Integer, String> rddData = sc.parallelizePairs(data);
       rddData.saveAsTextFile("data");
       rddData.saveAsObjectFile("data1");
        sc.close();
    }
}
