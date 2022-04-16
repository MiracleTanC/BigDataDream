package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.net.URL;
import java.util.*;

@Service("DemoSortByKeyService")
public class DemoSortByKeyService extends BaseJob implements IProcessService {
    /**
     * 在一个(K,V)的 RDD 上调用，K 必须实现 Ordered 接口(特质)，返回一个按照 key 进行排序的
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("SortByKyDemo");
        List<Tuple2<String,Integer>> data2 = Arrays.asList(new Tuple2<String,Integer>("e",3),new Tuple2<String,Integer>("b",5),new Tuple2<String,Integer>("c",6));
        JavaPairRDD<String, Integer> sjrdd2 = sc.parallelizePairs(data2);
        JavaPairRDD<String, Integer> rss = sjrdd2.sortByKey();// asc true false
        System.out.println(rss.collect());
//        JavaPairRDD<String, Integer> rss2 = sjrdd2.sortByKey(new Comparator<String>() {
//            @Override
//            public int compare(String o1, String o2) {
//                return 0;
//            }
//        });// asc true false
        //按值排序
        JavaPairRDD<String, Integer> rss2 = sjrdd2.mapToPair(n->new Tuple2<Integer,String>(n._2,n._1))
                .sortByKey(false).mapToPair(n->new Tuple2<String,Integer>(n._2,n._1));
        System.out.println(rss2.collect());
        sc.close();
    }
}
