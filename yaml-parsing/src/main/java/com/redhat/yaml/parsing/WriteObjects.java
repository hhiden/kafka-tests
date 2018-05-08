/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.yaml.parsing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;

/**
 *
 * @author hhiden
 */
public class WriteObjects {
    public static void main(String[] args){
        try {
            StreamProcessingStep s1 = new StreamProcessingStep();
            s1.setName("Window");
            s1.setLabel("Join events into a window");
            s1.setInputStreams(new String[]{"S1", "S2"});
            s1.setOutputStreams(new String[]{"W"});
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File("/work/step.yml"), s1);
            
            File inFile = new File("/work/step.yml");
            StreamProcessingStep recreated = mapper.readValue(inFile, StreamProcessingStep.class);
            System.out.println(recreated.getLabel());
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
