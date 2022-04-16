package com.example.bigdatadream.service.impl;

import cn.hutool.json.JSONUtil;
import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.stereotype.Service;
import scala.xml.Null;

import java.net.URL;
import java.util.*;

/**
 * map和mapPartitions
 * map	mapPartitions
 * transformation	transformation
 * 基于一行进行操作	基于一个分区的数据操作
 * 没处理完一行就返回一个对象	处理完一个分区的所有行才返回
 * 不将输出结果保存在内存中	输出保留在内存中，因为它可以在处理完所有行后返回
 * 易于实例化服务（可重用对象）	易于实例化服务（可重用对象）
 * 无法确定何时结束服务（No CleanupMethod）	返回前可以关闭服务
 * mapPartitions要点
 * mapPartitions的优势
 * mapPartitions是一个特殊的map，它在每个分区中只调用一次。
 * 由于它是针对每个分区进行处理，所以，它在数据处理过程中产生的对象会远小于map产生的对象。
 * 当需要处理的数据量很大时mapPartitions不会把数据都加载到内存中，避免由于数据量过大而导致的内存不足的错误。
 * mapPartitions为了提升运行的效率，在数据处理时它还会进行优化，把该放的数据放到内存中，把其他一些数据放到磁盘中。
 * mapPartitions函数的处理是通过迭代器进行的，输出的也是迭代器，通过输入参数（Iterarator [T]），各个分区的全部内容都可以作为值的顺序流。
 * 自定义函数必须返回另一个Iterator [U]。组合的结果迭代器会自动转换为新的RDD。
 * mapPartitions函数返回的是一个RDD类，具体来说是一个：MapPartitionsRDD。
 * 和map不同，由于 mapPartitions是基于每个数据的分区进行处理的，所以在生成对象时也会基于每个分区来生成，而不是针对每条记录来生成。例如：若需要连接外部数据库比如：hbase或mysql等，只会针对每个分区生成一个连接对象。
 * mapPartitions要注意的问题
 * mapPartitions是针对每个分区进行处理的，若最后的结果想要得到一个全局范围内的，需要慎重考虑
 */
@Service("DemoMapPartitionsService")
public class DemoMapPartitionsService extends BaseJob implements IProcessService {
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("map");
        List<Integer> data = Arrays.asList(new Integer[]{1, 2, 3, 4,5,6,7});
        JavaRDD<Integer> dataRdd1 = sc.parallelize(data,2);
        JavaRDD<Integer> integerJavaRDD = dataRdd1.mapPartitions(n->{
            List<Integer> rss = new ArrayList<>();
            while (n.hasNext()) {
                rss.add(n.next()*2);
            }
            return rss.iterator();
        });
        System.out.println(integerJavaRDD.collect());//[2, 4, 6, 8, 10, 12, 14]

        //只要第二个分区的数据
        JavaRDD<Integer> integerJavaRDD2 = dataRdd1.mapPartitionsWithIndex((index,n)->{
            List<Integer> rss = new ArrayList<>();
            if(index==1){
                while (n.hasNext()) {
                    rss.add(n.next());
                }
            }
            return rss.iterator();

        },false);
        System.out.println(integerJavaRDD2.collect());//[4, 5, 6, 7]

        //打印数据分区号及元素
        List<Integer> data2 = Arrays.asList(new Integer[]{1, 2, 3, 4,5,6,7,8,9});
        JavaRDD<Integer> dataRdd2 = sc.parallelize(data2);//local[*]默认8个分区
        JavaRDD<Map<Integer,Integer>> mapJavaRDD = dataRdd2.mapPartitionsWithIndex((index,n)->{
            List<Map<Integer,Integer>> rss = new ArrayList<>();
            while (n.hasNext()) {
                Map<Integer,Integer> item=new HashMap<>();
                item.put(index,n.next());
                rss.add(item);
            }
            return rss.iterator();
        },false);
        System.out.println(mapJavaRDD.collect());//[{0=1}, {1=2}, {2=3}, {3=4}, {4=5}, {5=6}, {6=7}, {7=8}, {7=9}]
        stop(sc);
    }
}
