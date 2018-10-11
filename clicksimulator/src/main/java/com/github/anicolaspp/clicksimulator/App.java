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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class App {
    
    private static ObjectMapper MAPPER = new ObjectMapper();
    
    private static String TOPIC = "/user/mapr/streams/click_stream:all_links";
    
    private static ExecutorService executorService = Executors.newWorkStealingPool(10);
    
    public static void main(String[] args) {
        
        System.out.println("running...");
        
        val numberOfLinks = getNumberOfLinks(args);
        val producer = getProducer();
        
        val done = CompletableFuture.allOf(
                new Selector()
                        .getRandomLinksStream(new Respository())
                        .map(App::getLinkMessage)
                        .map(App::getProducerRecord)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .limit(numberOfLinks)
                        .map(sendRecord(producer))
                        .toArray(CompletableFuture[]::new)
        );
        
        done.join();
        
        System.out.println("done....");
    }
    
    private static Function<ProducerRecord<String, String>, CompletableFuture<Void>> sendRecord(
            KafkaProducer<String, String> producer) {
        
        return record -> CompletableFuture.runAsync(() -> sendRecord(producer, record), executorService);
    }
    
    private static Integer getNumberOfLinks(String[] args) {
        if (args.length < 1) {
            return Integer.MAX_VALUE;
        } else {
            return Integer.valueOf(args[0]);
        }
    }
    
    private static LinkMessage getLinkMessage(Link link) {
        return LinkMessage.builder()
                .path(link.value)
                .isHot(link.isHot)
                .build();
    }
    
    private static void sendRecord(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        System.out.println("sending: " + record.value());
        
        try {
            producer.send(record).get();
            
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

