package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;

import java.net.URL;
import java.util.*;

@Service("DemoSummaryService")
public class DemoSummaryService extends BaseJob implements IProcessService {
    /**
     * 案例实操
     * 数据准备：agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 需求描述：统计出每一个省份每个广告被点击数量排行的 Top3
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("summary");
        URL resource = this.getClass().getClassLoader().getResource("data/agent.log");
        String filePath=resource.getFile();
        JavaRDD<String> fileRDD = sc.textFile(filePath);
        //agent.log：列依次表示，时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
        //1.转换成键值对的形式 如（（省份，广告）,点击数）
        JavaPairRDD<Tuple2<String, String>, Integer> rddData = fileRDD.mapToPair(n -> {
            String[] info = n.split(" ");
            return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<>(info[1], info[4]), 1);
        });
        System.out.println(rddData.collect());
        //计算相同key的点击数和：（（省份，广告）,点击数和）
        JavaPairRDD<Tuple2<String, String>, Integer> rddData2 = rddData.reduceByKey((n, m) -> n + m);
        System.out.println(rddData2.collect());
        //转换key的结构 （（省份，广告）,点击数和）->(省份,(广告，点击数和))
        JavaPairRDD<String, Tuple2<String, Integer>> rddData3 = rddData2.mapToPair(n -> {
            Tuple2<String, String> kv = n._1;
            Tuple2<String, Integer> item = new Tuple2<>(kv._2, n._2);
            return new Tuple2<String, Tuple2<String, Integer>>(kv._1, item);
        });
        System.out.println(rddData3.collect());
        //按省分组 输出(省份,[(广告，点击数和),(广告，点击数和),(广告，点击数和)])
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> rddData4 = rddData3.groupByKey();
        System.out.println(rddData4.collect());
        //取元组里 [(广告，点击数和),(广告，点击数和),(广告，点击数和)] 按点击数和降序排列
        JavaPairRDD<String, List<Tuple2<String, Integer>>> rddData5 = rddData4.mapValues(n -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            Iterator<Tuple2<String, Integer>> iterator = n.iterator();
            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                list.add(next);
            }
            //重写排序比较方式，按照值比较
            list.sort((o1, o2) -> o2._2 - o1._2);
            List<Tuple2<String, Integer>> t = new ArrayList<>();
            Iterator<Tuple2<String, Integer>> iterator1 = list.iterator();
            //遍历排序好的集合，取前三
            Integer i = 0;
            Integer takeNum = 3;
            while (iterator1.hasNext() & i < takeNum) {
                t.add(iterator1.next());
                i++;
            }
            return t;
        });
        //输出打印
        for (Tuple2<String, List<Tuple2<String, Integer>>> tp : rddData5.collect()) {
            System.out.println(tp);
        };
        /**(省份id,[(广告id,点击次数),(广告id,点击次数),(广告id,点击次数)])
         * (4,[(12,25), (2,22), (16,22)])
         * (8,[(2,27), (20,23), (11,22)])
         * (6,[(16,23), (24,21), (22,20)])
         * (0,[(2,29), (24,25), (26,24)])
         * (2,[(6,24), (21,23), (29,20)])
         * (7,[(16,26), (26,25), (1,23)])
         * (5,[(14,26), (21,21), (12,21)])
         * (9,[(1,31), (28,21), (0,20)])
         * (3,[(14,28), (28,27), (22,25)])
         * (1,[(3,25), (6,23), (5,22)])
         */
        sc.close();
    }
}
