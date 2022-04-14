package com.example.bigdatadream.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

@Slf4j
public abstract class BaseJob {
    public JavaSparkContext initSpark(String appName){
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        log.info("spark初始化成功");
        return sparkContext;
    }
    public void stop(JavaSparkContext context){
        if(context!=null){
            context.close();
        }
    }
}
