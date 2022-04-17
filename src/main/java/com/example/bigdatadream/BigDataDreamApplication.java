package com.example.bigdatadream;

import com.example.bigdatadream.service.impl.action.*;
import com.example.bigdatadream.service.impl.transformation.*;
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
    @Autowired
    private DemoSortByService demoSortByService;
    @Autowired
    private DemoIntersectionService demoIntersectionService;
    @Autowired
    private DemoUnionService demoUnionService;
    @Autowired
    private DemoSubtractService demoSubtractService;
    @Autowired
    private DemoZipService demoZipService;
    @Autowired
    private DemoPartitionByService demoPartitionByService;
    @Autowired
    private DemoReduceByKeyService demoReduceByKeyService;
    @Autowired
    private DemoGroupByKeyService demoGroupByKeyService;
    @Autowired
    private DemoAggregateByKeyService demoAggregateByKeyService;
    @Autowired
    private DemoFoldByKeyService demoFoldByKeyService;
    @Autowired
    private DemoCombineByKeyService demoCombineByKeyService;
    @Autowired
    private DemoSortByKeyService demoSortByKeyService;
    @Autowired
    private DemoJoinService demoJoinService;
    @Autowired
    private DemoLeftOuterJoinService demoLeftOuterJoinService;
    @Autowired
    private DemoCoGroupService demoCoGroupService;
    @Autowired
    private DemoSummaryService demoSummaryService;
    @Autowired
    private DemoReduceService demoReduceService;
    @Autowired
    private DemoCollectService demoCollectService;
    @Autowired
    private DemoCountService demoCountService;
    @Autowired
    private DemoFirstService demoFirstService;
    @Autowired
    private DemoTakeService demoTakeService;
    @Autowired
    private DemoTakeOrderedService demoTakeOrderedService;
    @Autowired
    private DemoAggregateService demoAggregateService;
    @Autowired
    private DemoFoldService demoFoldService;
    @Autowired
    private DemoCountByKeyService demoCountByKeyService;
    @Autowired
    private DemoSaveService demoSaveService;
    @Autowired
    private DemoForeachService demoForeachService;
    public static void main(String[] args) {
        SpringApplication.run(BigDataDreamApplication.class, args);
    }

    @Override
    public void run(String... args){
        //wordCountService.process();
        //demoMapService.process();
        //demoFlatMapService.process();
        //demoGlomService.process();
        //demoGroupByService.process();
        //demoFilterService.process();
        //demoSampleService.process();
        //demoDistinctService.process();
        //demoMapPartitionsService.process();
        //demoCoalesceService.process();
        //demoSortByService.process();
        //demoIntersectionService.process();
        //demoUnionService.process();
        //demoSubtractService.process();
        //demoZipService.process();
        //demoPartitionByService.process();
        //demoReduceByKeyService.process();
        //demoGroupByKeyService.process();
        //demoAggregateByKeyService.process();
        //demoFoldByKeyService.process();
        //demoCombineByKeyService.process();
        //demoSortByKeyService.process();
        //demoJoinService.process();
        //demoLeftOuterJoinService.process();
        //demoCoGroupService.process();
        //demoSummaryService.process();
        //demoReduceService.process();
        //demoCollectService.process();
        //demoCountService.process();
        //demoFirstService.process();
        //demoTakeService.process();
        //demoTakeOrderedService.process();
        //demoFoldService.process();
        //demoCountByKeyService.process();
        //demoSaveService.process();
       demoForeachService.process();
    }
}
