package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service("DemoSampleService")
public class DemoSampleService extends BaseJob implements IProcessService {
    /**
     * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
     * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("sampleDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //指定3个分区
        JavaRDD<Integer> rddData = sc.parallelize(data);
        // 抽取数据不放回（伯努利算法）
        // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
        // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
        // 第一个参数：抽取的数据是否放回，false：不放回
        // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
        // 第三个参数：随机数种子
        JavaRDD<Integer> filterData = rddData.sample(false,0.5);
        System.out.println(filterData.collect());
        // 抽取数据放回（泊松算法）
        // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
        // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
        // 第三个参数：随机数种子
        JavaRDD<Integer> filterData2 = rddData.sample(true,2);
        System.out.println(filterData2.collect());

        sc.close();
    }
}
