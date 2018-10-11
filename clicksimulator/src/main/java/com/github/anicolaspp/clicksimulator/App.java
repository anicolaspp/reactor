package com.github.anicolaspp.clicksimulator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class App {
    
    private static ObjectMapper MAPPER = new ObjectMapper();
    
    private static String TOPIC = "/user/mapr/streams/click_stream:all_links";
    
    public static void main(String[] args) {
    
        System.out.println("running...");
        
        val numberOfLinks = getNumberOfLinks(args);
        val producer = getProducer();
    
        val done = CompletableFuture.allOf(new Selector()
                .getRandomLinksStream(new Respository())
                .map(App::getLinkMessage)
                .map(App::getProducerRecord)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .limit(numberOfLinks)
                .map(record -> CompletableFuture.runAsync(() -> sendRecord(producer, record)))
                .toArray(CompletableFuture[]::new));
    
        done.join();
    
        System.out.println("done....");
    }
    
    private static Integer getNumberOfLinks(String[] args) {
        if (args.length < 1) {
            return Integer.MAX_VALUE;
        } else {
            return Integer.valueOf(args[0]);
        }
    }
    
    private static LinkMessage getLinkMessage(Link link) {
        return LinkMessage.builder().path(link.value).isHot(link.isHot).build();
    }
    
    private static void sendRecord(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        System.out.println("sending: " + record.value());
        
        try {
            val meta = producer.send(record).get();
            
            System.out.println(meta.topic());
            System.out.println(meta.partition());
            System.out.println(meta.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
    
    private static Optional<ProducerRecord<String, String>> getProducerRecord(LinkMessage linkMessage) {
        try {
            String jsonInString = MAPPER.writeValueAsString(linkMessage);
            return Optional.of(new ProducerRecord<>(TOPIC, jsonInString));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }
    
    private static KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.setProperty("batch.size", "16384");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("block.on.buffer.full", "true");
        
        return new KafkaProducer<>(props);
    }
}

