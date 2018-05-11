package com.redhat.simple.processor;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.annotations.ServiceParameter;
import com.redhat.processor.annotations.SourceType;
import javax.json.JsonObject;

/**
 * Process messages
 * @author hhiden
 */
@MessageProcessor(
        configSource = SourceType.SPECIFIED,
        serverName = "localhost",
        port = "9092")
public class SimpleProcessor {
    @ServiceParameter(name = "MY_PARAMETER")
    private String param;
    
    @HandleMessage(
            outputType = OutputType.TOPIC, 
            outputName = "output-data", 
            inputName = "input-data",
            inputGroupName = "processor-group",
            outputClientId = "my-processor",
            configSource = SourceType.SPECIFIED)
    public JsonObject filterDate(JsonObject message){
        return message;
    }
}