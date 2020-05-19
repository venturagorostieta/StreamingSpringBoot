package com.streaming.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.streaming.service.KafkaProducer;
import com.streaming.service.SparkLikeService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class StreamingController {
	
	private final KafkaProducer producer;

    private final SparkLikeService sparkLikeService;

    @RequestMapping("/send-like")
    public String sendLike(@RequestParam(value = "post_id", required = true) Integer postId) {
        producer.send(postId);
        return "test1";
    }

    @RequestMapping("/launch")
    public String launch() {
        sparkLikeService.launch();
        return "test2";
    }

}
