package com.redhat.processor.container.kafka;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.container.ContainerUtils;
import com.redhat.processor.container.MessageHandler;
import com.redhat.processor.container.MessageHandlerContainer;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 * This class handles messages for a single stream in a processor class
 * @author hhiden
 */
public class KafkaMessageHandler extends MessageHandler implements Runnable {
    private static final Logger logger = Logger.getLogger(KafkaMessageHandler.class.getName());
    
    private final boolean outputStreamPresent;
    private final String outputStreamName;
    private final String inputStreamName;
    private final String inputGroupName;
    private final String outputClientId;

    private Consumer<Long, byte[]> inputConsumer;
    private Producer<Long, byte[]> outputProducer;
    
    private volatile boolean shutdownFlag = false;

    public KafkaMessageHandler(MessageHandlerContainer parent, Object handler, Method m, HandleMessage config) {
        super(handler, m, parent, config);
        
        // Sort out inputs
        this.parent = parent;
        this.handler = handler;
        this.handlerMethod = m;
        inputStreamName = ContainerUtils.resolve(config.configSource(), config.inputName());
        inputGroupName = config.inputGroupName();
        logger.info("Using handler input stream: " + inputStreamName + "[" + inputGroupName + "]");
        if (config.outputType() == OutputType.TOPIC) {
            outputStreamPresent = true;
            outputStreamName = ContainerUtils.resolve(config.configSource(), config.outputName());
            outputClientId = config.outputClientId();
            logger.info("Using hander output stream: " + outputStreamName);
        } else {
            outputStreamPresent = false;
            outputStreamName = "";
            outputClientId = "";
            logger.info("Handler has no output stream requirement");
        }
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
    /** Create a Kafka consumer attached to a queue */
    private Consumer<Long, byte[]> createConsumer(String groupName, String topicName) {
        logger.info("Creating Kafka consumer for Topic: " + topicName);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.getServerName() + ":" + parent.getServerPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Consumer<Long, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }
    
    /** Create a Kafka producer for a queue */
    private Producer<Long, byte[]> createProducer(String groupName) {
        logger.info("Creating Kafka producer with ClientID: " + groupName);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.getServerName() + ":" + parent.getServerPort());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, groupName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        
        return new KafkaProducer<>(props);
    }   
    /**
     * Process messages and invoke the handler method
     */
    @Override
    public void run() {
        logger.info("Starting KafkaMessageHander.run");
        // Connect the input
        inputConsumer = createConsumer(inputGroupName, inputStreamName);

        // Connect the output if there is one
        if(outputStreamPresent){
            outputProducer = createProducer(outputClientId);
        }        
        while(!shutdownFlag){
            // Consume messages
            final ConsumerRecords<Long, byte[]> consumerRecords
                    = inputConsumer.poll(1);

            // Send each one through the message
            for(ConsumerRecord<Long, byte[]> record : consumerRecords){
                try {
                    Object callData = ContainerUtils.deserialize(record.value());
                    if(outputStreamPresent){
                        // Retrieve the output
                        Object returnData = handlerMethod.invoke(handler, callData);
                        
                        // Push back to the stream
                        if(returnData!=null){
                            byte[] returnByteData = ContainerUtils.serialize(returnData);

                            ProducerRecord<Long, byte[]> outputRecord = new ProducerRecord<>(outputStreamName, System.nanoTime(), returnByteData);

                            RecordMetadata metadata = outputProducer.send(outputRecord).get();
                            logger.info("Sent:" + metadata.toString());
                        } else {
                            logger.info("Processor did not create a message");
                        }
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