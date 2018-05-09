
package com.redhat.processor.example;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.annotations.SourceType;

/**
 * Consumes messages from specified queue
 * @author hhiden
 */
@MessageProcessor(
        configSource = SourceType.SPECIFIED,
        serverName = "localhost",
        port = 9092)
public class SimpleConsumer {
    @HandleMessage(
            outputType = OutputType.TOPIC, 
            outputName = "output-data", 
            inputName = "input-data",
            inputGroupName = "processor-group",
            outputClientId = "my-processor",
            configSource = SourceType.SPECIFIED)
    public Object process(Object message){
       String msg = (String)message;
       return msg + "_processed";
    }
}