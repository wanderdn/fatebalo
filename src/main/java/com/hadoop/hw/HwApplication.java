package com.hadoop.hw;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
@SpringBootApplication
public class HwApplication {

    public static void main(String[] args) {

        SpringApplication.run(HwApplication.class, String[]args)
    }
}
