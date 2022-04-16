package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mortbay.util.ajax.JSON;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.net.URL;
import java.util.Arrays;
import java.util.List;

@Service("WordCountService")
public class WordCountService extends BaseJob implements IProcessService {
    @Override
    public void process() {
        JavaSparkContext sparkContext = initSpark("wordCount");
        URL resource = this.getClass().getClassLoader().getResource("data/wordcount.txt");
        String filePath=resource.getFile();
        JavaRDD<String> fileRDD = sparkContext.textFile(filePath);
        // 将文件中的数据进行分词
        JavaRDD<String> wordRDD = fileRDD.flatMap(n -> Arrays.asList(n.split("\n| ")).iterator());
        // 转换数据结构 word => (word, 1)
        JavaPairRDD<String, Integer> wordPair = wordRDD.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairRDD<String, Integer> word2CountRDD = wordPair.reduceByKey((integer1, integer2) -> integer1 + integer2);
        JavaPairRDD<String, Integer> word2Count = word2CountRDD.filter(tuple2 -> tuple2._1.length() > 0)
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._1, tuple2._2))  //单词与频数倒过来为新二元组，按频数倒排序取途topK
                .sortByKey(false);
        List<Tuple2<String, Integer>> collect = word2Count.collect();
        for (Tuple2<String, Integer> tuple2 : collect) {
            System.out.println(tuple2._1()+":"+tuple2._2());
        };
        //关闭spark上下文
        stop(sparkContext);
    }
}
