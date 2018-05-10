package com.redhat.simple.processor;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.annotations.SourceType;

/**
 * Process messagesz
 * @author hhiden
 */
@MessageProcessor(
        configSource = SourceType.SPECIFIED,
        serverName = "localhost",
        port = 9092)
public class SimpleProcessor {
    @HandleMessage(
            outputType = OutputType.TOPIC, 
            outputName = "output-data", 
            inputName = "input-data",
            inputGroupName = "processor-group",
            outputClientId = "my-processor",
            configSource = SourceType.SPECIFIED)
    public Object filterDate(Object message){
        return message + "_filtered";
    }
}