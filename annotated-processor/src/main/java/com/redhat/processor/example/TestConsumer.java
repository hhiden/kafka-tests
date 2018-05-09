/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.processor.example;

import com.redhat.processor.container.MessageHandlerContainer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hhiden
 */
public class TestConsumer {
    private static final Logger logger = Logger.getLogger("TestConsumer");
    
    public static void main(String[] args){
        SimpleConsumer consumer = new SimpleConsumer();
        
        System.setProperty("KAFKA_TOPIC_INPUT", "my-topic");
        System.setProperty("KAFKA_TOPIC_OUTPUT", "output-topic");
        
        MessageHandlerContainer container = new MessageHandlerContainer(consumer);
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            container.shutdown();
         
            System.out.println("SHUTDOWN");
        }));        
    }
}
