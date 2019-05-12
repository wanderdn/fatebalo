package com.hadoop.hw;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConf {

    @Bean
    public JavaSparkContext sc() {
        org.apache.spark.SparkConf conf = new org.apache.spark.SparkConf();
        conf.setAppName("bigramms");
//        conf.setMaster("spark://localhost:8088");
        return new JavaSparkContext(conf);
    }

}

