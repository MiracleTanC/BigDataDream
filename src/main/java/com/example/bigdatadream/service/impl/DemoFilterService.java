package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.*;

@Service("DemoFilterService")
public class DemoFilterService extends BaseJob implements IProcessService {
    /**
     * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
     * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("glomDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //指定3个分区
        JavaRDD<Integer> rddData = sc.parallelize(data);

        JavaRDD<Integer> filterData = rddData.filter(n -> n > 5);
        System.out.println(filterData.collect());
        JavaRDD<Integer> filterData2 = rddData.filter(n -> n %2 ==1);
        System.out.println(filterData2.collect());
        URL resource = this.getClass().getClassLoader().getResource("data/apache.log");
        if (resource != null) {
            String filePath = resource.getFile();
            JavaRDD<String> fileRDD = sc.textFile(filePath);
            //从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
            JavaRDD<String> mapJavaRDD = fileRDD.filter(m -> {
                String[] arr = m.split(" ");
                String[] dateArr = arr[3].split(":");
                return Objects.equals(dateArr[0], "17/05/2015");
            });
            System.out.println(mapJavaRDD.collect());
        }
        sc.close();
    }
}
