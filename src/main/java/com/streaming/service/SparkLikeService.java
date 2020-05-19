package com.streaming.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Service
@Slf4j
public class SparkLikeService  implements Serializable{
	
	 private static final long serialVersionUID = 3L;

	    @Value("${like.topic}")
	    private String topic;

	    public void launch() {
	        SparkConf conf = new SparkConf()
	                .setAppName("SparkLikes")
	                .setMaster("local[*]")
	                .set("spark.default.parallelism", "15")
	                .set("spark.streaming.concurrentJobs", "5")
	                .set("spark.executor.memory", "1G")
	                .set("spark.cores.max", "2")
	                .set("spark.local.dir", "/tmp/mySparkLikes")
	                .set("spark.streaming.kafka.maxRatePerPartition", "5");

			Collection<String> topics = Arrays.asList(topic);// 1 o more topics


			Map<String, Object> kafkaParams = new HashMap<>();
			kafkaParams.put("bootstrap.servers", "localhost:9092");// 1 or more brokers
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "spring-boot");
			kafkaParams.put("auto.offset.reset", "latest");
			kafkaParams.put("enable.auto.commit", false);

	        JavaStreamingContext jsc = new JavaStreamingContext(
	                new JavaSparkContext(conf),
	                Durations.seconds(3));
	        jsc.sparkContext().setLogLevel("ERROR");

	        jsc.checkpoint("checkpoint"); 

	        
	        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
	        

	        System.out.println("stream started!");
	        stream.print();
	    	JavaDStream<String> lines = stream.map(x -> x.value());
	    	
	    	JavaPairDStream<String, Integer> countDStream =lines.mapToPair(f -> new Tuple2<>(f, 1)).reduceByKey(Integer::sum);	    				    			

//	        countDStream.foreachRDD(v -> {
//	            v.foreach(record -> {
//	                String sql = String.format("UPDATE `post` SET likes = likes + %s WHERE id=%d", record._2, record._1);
//	                System.out.println(sql);
//	            });
//	          
//	            log.info("	Se procesa un lote de flujo de datos: {} ", v);
//	        });
	    	
	    	countDStream.print();
	        jsc.start();
	    }
}
