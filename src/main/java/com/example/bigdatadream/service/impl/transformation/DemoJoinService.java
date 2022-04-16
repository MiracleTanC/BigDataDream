package com.example.bigdatadream.service.impl.transformation;

import com.example.bigdatadream.service.IProcessService;
import com.example.bigdatadream.service.impl.BaseJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Service("DemoJoinService")
public class DemoJoinService extends BaseJob implements IProcessService {
    /**
     * 在类型为(K,V)和(K,W)的RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
     * (K,(V,W))的RDD
     */
    @Override
    public void process() {
        JavaSparkContext sc = initSpark("joinDemo");
        List<Tuple2<Integer,String>> data1 = Arrays.asList(new Tuple2<Integer,String>(1,"a"),new Tuple2<Integer,String>(2,"b"),new Tuple2<Integer,String>(3,"c"));
        List<Tuple2<Integer,String>> data2 = Arrays.asList(new Tuple2<Integer,String>(1,"aa"),new Tuple2<Integer,String>(2,"bb"),new Tuple2<Integer,String>(3,"cc"));
        JavaPairRDD<Integer,String> sjrdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer,String> sjrdd2 = sc.parallelizePairs(data2);
        sjrdd1.join(sjrdd2).collect().forEach(System.out::println);
        /**
         * (1,(a,aa))
         * (2,(b,bb))
         * (3,(c,cc))
         */
        //如果 key 存在不相等则不会匹配上
        //[(1,a),(2,b),(3,c)] 和[(1,aa),(5,bb),(3,cc)] 结果 [(1,(a,aa)),(3,(c,cc))]
        //若相同的key有多个，则会逐条匹配，可能出现笛卡尔积，数据量几何倍增长，谨慎使用
        //[(1,a),(2,b),(3,c)] 和[(1,aa),(1,aaa),(1,aaaa),(2,bb),(3,cc)]结果[(1,(a,aa)),(1,(a,aaa)),(1,(a,aaaa)),(2,(b,bb)),(3,(c,cc))]
        sc.close();
    }
}
