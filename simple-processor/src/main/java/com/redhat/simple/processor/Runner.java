package com.redhat.simple.processor;

import com.redhat.processor.container.MessageHandlerApplication;

/**
 * Runs a processor - not needed if the MessageHandlerApplication is used as the
 * main-class in a deployed app.
 * @author hhiden
 */
public class Runner {
    public static void main(String[] args){
        try {
            MessageHandlerApplication app = new MessageHandlerApplication();
            app.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                app.shutdown();
                System.out.println("SHUTDOWN");
            }));                
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}