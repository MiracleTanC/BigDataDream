package com.example.bigdatadream;

import com.example.bigdatadream.service.impl.WordCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BigDataDreamApplication implements CommandLineRunner {

    @Autowired
    private WordCountService wordCountService;
    public static void main(String[] args) {
        SpringApplication.run(BigDataDreamApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        wordCountService.process();
    }
}
