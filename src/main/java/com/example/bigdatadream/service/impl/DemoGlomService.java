package com.example.bigdatadream.service.impl;

import com.example.bigdatadream.service.IProcessService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.*;

@Service("DemoGlomService")
public class DemoGlomService extends BaseJob implements IProcessService {
    /**
     * glom(function) 算子：
     * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
     * glom的作用是将同一个分区里的元素合并到一个array里
     * glom属于Transformation算子：这种变换并不触发提交作业，完成作业中间过程处理。
     * Transformation 操作是延迟计算的，也就是说从一个RDD 转换生成另一个 RDD 的转换操作不是马上执行，需要等到有 Action 操作的时候才会真正触发运算。
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("glomDemo");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //指定3个分区
        JavaRDD<Integer> rddData = sc.parallelize(data, 3);
        //Integer reduce = rddData.reduce((value1, value2) -> Math.max(value1, value2));
        Integer max = rddData.reduce(Math::max);
        Integer min = rddData.reduce(Math::min);
        //Integer sum = rddData.reduce((value1, value2) -> value1+value2);
        Integer sum = rddData.reduce(Integer::sum);
        long avg = sum/rddData.count();
        System.out.printf("max:%s,min:%s,sum:%s,avg:%s",max,min,sum,avg);
        JavaRDD<List<Integer>> glom1 = rddData.glom();
        System.out.println(glom1.collect());
        //结果 [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
        //先求分区最大，然后分区最大比大小
        Integer reduceMax = glom1.map(n -> n.stream().max(Integer::compare).get()).reduce(Math::max);
        Integer reduceMin = glom1.map(n -> n.stream().min(Integer::compare).get()).reduce(Math::min);
        //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        Integer reduceSum = glom1.map(n -> n.stream().max(Integer::compare).get()).reduce(Integer::sum);
        System.out.printf("max=%s,min=%s,sum=%s\n",reduceMax,reduceMin,reduceSum);
        sc.close();
    }
}
