/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.simple.processor;

import com.redhat.processor.StreamingContainerApplication;

/**
 *
 * @author hhiden
 */
public class Runner {
     public static void main(String[] args){
        try {
            System.setProperty("MY_PARAMETER", "55");
            StreamingContainerApplication app = new StreamingContainerApplication();
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
