package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoCombineByKeyService")
public class DemoCombineByKeyService extends BaseJob implements IProcessService {
    /**
     * 最通用的对key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。
     * 类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("CombineByKey");

        List<Tuple2<String,Integer>> data2 = Arrays.asList(new Tuple2<String,Integer>("a",4),new Tuple2<String,Integer>("b",5),new Tuple2<String,Integer>("c",6));
//        JavaRDD<Tuple2<String, Integer>> jrdd2 = sc.parallelize(data2);
//        JavaPairRDD<String, Integer> sjrdd2 = jrdd2.mapToPair(n -> new Tuple2<String, Integer>(n._1, n._2));
        JavaPairRDD<String, Integer> sjrdd2=sc.parallelizePairs(data2);
        JavaPairRDD<String, Tuple2<Integer, Integer>> newJavaRdd = sjrdd2.combineByKey(n -> {
            return new Tuple2<Integer, Integer>(n, 1);
        }, (t, v) -> {
            return new Tuple2<Integer, Integer>(t._1 + v, t._2 + 1);
        }, (t1, t2) -> {
            return new Tuple2<Integer, Integer>(t1._1 + t2._1, t1._2 + t2._2);
        });
        System.out.println(newJavaRdd.collect());//[(a,(4,1)), (b,(5,1)), (c,(6,1))]

        /**
         * reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别？
         * reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
         * FoldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
         * AggregateByKey：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
         * CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
         */
        sc.close();
    }
}
