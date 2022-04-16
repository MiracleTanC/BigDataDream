package com.example.bigdatadream;

import com.example.bigdatadream.service.impl.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BigDataDreamApplication implements CommandLineRunner {

    @Autowired
    private WordCountService wordCountService;
    @Autowired
    private DemoMapService demoMapService;
    @Autowired
    private DemoMapPartitionsService demoMapPartitionsService;
    @Autowired
    private DemoFlatMapService demoFlatMapService;
    @Autowired
    private DemoGlomService demoGlomService;
    @Autowired
    private DemoGroupByService demoGroupByService;
    @Autowired
    private DemoFilterService demoFilterService;
    @Autowired
    private DemoSampleService demoSampleService;
    @Autowired
    private DemoDistinctService demoDistinctService;
    @Autowired
    private DemoCoalesceService demoCoalesceService;
    public static void main(String[] args) {
        SpringApplication.run(BigDataDreamApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        //wordCountService.process();
        //demoMapService.process();
        //demoFlatMapService.process();
        //demoGlomService.process();
        //demoGroupByService.process();
        //demoFilterService.process();
        //demoSampleService.process();
        //demoDistinctService.process();
        //demoMapPartitionsService.process();
        demoCoalesceService.process();
    }
}
