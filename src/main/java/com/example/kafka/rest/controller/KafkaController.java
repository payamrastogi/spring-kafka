package com.example.kafka.rest.controller;

import javax.websocket.server.PathParam;

import com.example.kafka.model.Greeting;
import com.example.kafka.service.Producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private Producer producer;

    @PostMapping("/publish/greeting")
    public void publishMessage(@RequestBody Greeting greeting){
        this.producer.sendGreetingMessage(greeting);
    }

    @PostMapping("/publish/partition/{partition}")
    public void publishMessage(@PathVariable ("partition") int partition, 
                                @RequestBody String message){
        this.producer.sendMessageToPartion(message, partition);
    }
}