package com.redhat.simple.processor;

import com.redhat.processor.annotations.MessageSource;
import com.redhat.processor.annotations.ProduceMessage;
import com.redhat.processor.annotations.SourceType;

/**
 * Simple timed message source
 * @author hhiden
 */
@MessageSource(configSource = SourceType.SPECIFIED, serverName = "localhost",port = "9092")
public class SimpleSource {
    
    @ProduceMessage(configSource = SourceType.SPECIFIED, interval = "1000", outputClientId = "test-source", outputName = "input-data")
    public String produceMessage(){
        return "Hello: " + System.currentTimeMillis();
    }
}