package com.streaming.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
	 private final KafkaTemplate<String, String> kafkaTemplate;

	    @Value("${like.topic}")
	    private String topic;

	    KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
	        this.kafkaTemplate = kafkaTemplate;
	    }

	    public void send(Integer postId) {
	        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, postId.toString(), "1");
	        this.kafkaTemplate.send(producerRecord);
	        System.out.println("Sent sample postId [" + postId + "] to " + topic);
	    }
}
