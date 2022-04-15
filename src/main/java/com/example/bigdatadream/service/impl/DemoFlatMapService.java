package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.*;

@Service("DemoFlatMapService")
public class DemoFlatMapService extends BaseJob implements IProcessService {
    /**
     * flatMap(function) 算子：
     * 通过function函数将RDD中的每一个元素转换为多个新的元素,并返回一个新的RDD。
     * <p>
     * 与map区别：
     * map一个元素经过函数计算后产生一个元素。
     * flatmap是一个元素元素经过计算后产生多个元素。
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("flatmapDemo");
        List<Integer> data1 = Arrays.asList(1, 2, 3);
        List<String> data2 = Arrays.asList("i am a superman", "you are superwomen");
        JavaRDD<Integer> rddData1 = sc.parallelize(data1);

//        JavaRDD<Integer> uJavaRDD1 = rddData1.flatMap(new FlatMapFunction<Integer, Integer>() {
//            @Override
//            public Iterator<Integer> call(Integer integer) throws Exception {
//                List<Integer> list2 = new ArrayList<>();
//                for (int i = 0; i <= integer; i++) {
//                    list2.add(i);
//                }
//                return list2.iterator();
//            }
//        });
        //flatMap算子lambda形式
        JavaRDD<Integer> uJavaRDD1 = rddData1.flatMap(n -> {
            List<Integer> list2 = new ArrayList<>();
            for (int i = 0; i <= n; i++) {
                list2.add(i);
            }
            return list2.iterator();
        });
        System.out.println(uJavaRDD1.collect());

        JavaRDD<String> rddData2 = sc.parallelize(data2);
//        JavaRDD<String> uJavaRDD2 = rddData2.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                List<String> list = new ArrayList<>();
//                String[] sArr = s.split(" ");
//                Collections.addAll(list, sArr);
//                return list.iterator();
//            }
//        });
        JavaRDD<String> uJavaRDD2 = rddData2.flatMap(n->{
            List<String> list = new ArrayList<>();
            String[] sArr = n.split(" ");
            Collections.addAll(list, sArr);
            return list.iterator();
        });
        System.out.println(uJavaRDD2.collect());
        //读取本地文件,读文件时flatMap是按行读取。
        URL resource = this.getClass().getClassLoader().getResource("data/city_info.txt");
        if (resource != null) {
            String filePath = resource.getFile();
            JavaRDD<String> fileRDD = sc.textFile(filePath);
            JavaRDD<Map<String, String>> mapJavaRDD = fileRDD.flatMap(n -> {
                List<Map<String, String>> ds = new ArrayList<>();
                String[] crr = n.split("\t");
                Map<String, String> mp = new HashMap<>();
                String key = crr[2];
                String value = crr[1];
                mp.put(key, value);
                ds.add(mp);
                return ds.iterator();
            });
            System.out.println(mapJavaRDD.collect());
        }
        sc.close();
    }
}
