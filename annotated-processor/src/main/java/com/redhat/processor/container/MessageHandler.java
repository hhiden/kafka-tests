package com.redhat.processor.container;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.OutputType;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author hhiden
 */
public class MessageHandler implements Runnable {
    private static final Logger logger = Logger.getLogger(MessageHandler.class.getName());
    
    private MessageHandlerContainer parent;
    private final boolean outputStreamPresent;
    private final String outputStreamName;
    private final String inputStreamName;
    private final String inputGroupName;
    private final String outputClientId;

    private Consumer<Long, byte[]> inputConsumer;
    private Producer<Long, byte[]> outputProducer;

    private Object handler;
    private Method handlerMethod;
    
    private volatile boolean shutdownFlag = false;

    public MessageHandler(MessageHandlerContainer parent, Object handler, Method m, HandleMessage config) {
        // Sort out inputs
        this.parent = parent;
        this.handler = handler;
        this.handlerMethod = m;
        inputStreamName = MessageHandlerContainer.resolve(config.configSource(), config.inputName());
        inputGroupName = config.inputGroupName();

        if (config.outputType() == OutputType.TOPIC) {
            outputStreamPresent = true;
            outputStreamName = MessageHandlerContainer.resolve(config.configSource(), config.outputName());
            outputClientId = config.outputClientId();
        } else {
            outputStreamPresent = false;
            outputStreamName = "";
            outputClientId = "";
        }
    }

    public void runConsumer() {
        inputConsumer = parent.createConsumer("KafkaExample", "input-data");
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<Long, byte[]> consumerRecords
                    = inputConsumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                } else {
                    continue;
                }
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), new String(record.value()),
                        record.partition(), record.offset());
            });
            inputConsumer.commitAsync();
        }
        inputConsumer.close();
        System.out.println("DONE");
    }

    public void shutdown() {
        shutdownFlag = true;
        if (inputConsumer != null) {
            inputConsumer.close();
        }

        if (outputProducer != null) {
            outputProducer.close();
        }
    }

    /**
     * Process messages and invoke the handler method
     */
    @Override
    public void run() {
        // Connect the input
        inputConsumer = parent.createConsumer(inputGroupName, inputStreamName);

        // Connect the output if there is one
        if(outputStreamPresent){
            outputProducer = parent.createProducer(outputClientId);
        }        
        while(!shutdownFlag){
            // Consume messages
            final ConsumerRecords<Long, byte[]> consumerRecords
                    = inputConsumer.poll(1);

            // Send each one through the message
            for(ConsumerRecord<Long, byte[]> record : consumerRecords){
                try {
                    Object callData = SerializationUtils.deserialize(record.value());
                    if(outputStreamPresent){
                        // Retrieve the output
                        Object returnData = handlerMethod.invoke(handler, callData);
                        
                        // Push back to the stream
                        byte[] returnByteData = SerializationUtils.serialize(returnData);
                        
                        ProducerRecord<Long, byte[]> outputRecord = new ProducerRecord<>(outputStreamName, System.nanoTime(), returnByteData);

                        RecordMetadata metadata = outputProducer.send(outputRecord).get();
                        logger.info("Sent:" + metadata.toString());
                    } else {
                        // Ignore the output
                        handlerMethod.invoke(handler, callData);
                        
                    }
                } catch (Exception e){
                    logger.log(Level.SEVERE, "Error running method", e);
                }
            }

            inputConsumer.commitAsync();                
        }

    }
}
