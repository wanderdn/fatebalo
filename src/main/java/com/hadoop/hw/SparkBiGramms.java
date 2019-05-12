package com.hadoop.hw;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Service
public class SparkBiGramms implements ApplicationListener<ContextRefreshedEvent>, Serializable {


    JavaSparkContext sc;
    private final String bigramsPath;

    public SparkBiGramms(JavaSparkContext sc, @Value("${spark.file}") String bigramsPath) throws IOException, InterruptedException {
        this.sc = sc;
        this.bigramsPath = bigramsPath;


    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        JavaRDD<String> lines = sc.textFile(bigramsPath);
        List<String> bigramms = new ArrayList<>();
        List<String> words = new ArrayList<>();
        lines.collect().stream().map(x -> x.toLowerCase().replaceAll(("[^\\d\\w]"), " "))
                .forEach(x ->Arrays.asList(x.split(" ")).stream().filter(s->!s.isEmpty()).forEach(words::add));
        Stream.iterate(0,x->x+1).limit(words.size()-2).filter(x->words.get(x).equals("narodnaya")).forEach(x->
                bigramms.add(words.get(x)+"_"+words.get(x+1)));

        JavaRDD<String> nGrams = sc.parallelize(bigramms);
        nGrams.mapToPair(w -> new Tuple2<>(w, 1)).reduceByKey(Integer::sum).coalesce(1).sortByKey().collect().forEach(tuple ->
                System.out.println(tuple._1.concat("\t").concat(tuple._2.toString())));

        sc.close();

    }

}




