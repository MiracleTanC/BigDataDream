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

@Service("DemoGroupByKeyService")
public class DemoGroupByKeyService extends BaseJob implements IProcessService {
    /**
     * 将数据源的数据根据 key 对 value 进行分组
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("GroupByKey");
        List<String> data = Arrays.asList("a", "b", "c", "d", "a","b","c");
        JavaRDD<String> rddData = sc.parallelize(data);
        JavaPairRDD<String, Integer> tuple2JavaRDD = rddData.mapToPair(n -> new Tuple2<String, Integer>(n, 1));
        System.out.println(tuple2JavaRDD.collect());
        JavaPairRDD<String, Iterable<Integer>> newJavaRdd = tuple2JavaRDD.groupByKey();
        System.out.println(newJavaRdd.collect());
        //和groupBy区别
        JavaPairRDD<String, Iterable<String>> newJavaRdd1 = rddData.groupBy(n -> n);
        System.out.println(newJavaRdd1.collect());
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> newJavaRdd2 = tuple2JavaRDD.groupBy(n -> n._1);
        System.out.println(newJavaRdd2.collect());
        /**
         * groupBy和groupByKey区别
         *         1:groupBy更灵活,可以指定元素分组
         *         2:groupByKey更高效 减少了网络传输，更适合用在大数据集上
         *         3:groupByKey只能接收键值对型集合
         *         4:groupBy底层实际上是重新把数据整理成KV形式 再调用groupByKey
         *              4.1:先调用map 转换成key,value(值为整个元素)
         *              4.2:再调用groupBykey
         */

        /**
         * 从 shuffle 的角度：
         *      reduceByKey 和 groupByKey 都存在 shuffle 的操作，
         *      但是reduceByKey 可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的数据量，
         *      而groupByKey 只是进行分组，不存在数据量减少的问题，reduceByKey 性能比较高。
         * 从功能的角度：
         *      reduceByKey 其实包含分组和聚合的功能。
         *      GroupByKey 只能分组，不能聚合，所以在分组聚合的场合下，推荐使用 reduceByKey，
         *      如果仅仅是分组而不需要聚合。那么还是只能使用groupByKey
         */
        sc.close();
    }
}
