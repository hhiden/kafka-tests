package com.redhat.processor.container;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.SourceType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 * Holds a message processor and finds queues etc
 * @author hhiden
 */
public class MessageHandlerContainer {
    private static final Logger logger = Logger.getLogger(MessageHandlerContainer.class.getName());
    private Object processorObject;
    
    private HashMap<String, MessageHandler> handlerMap = new HashMap<>();
    private String serverName;
    private int serverPort;

    public MessageHandlerContainer(Object processorObject) {
        this.processorObject = processorObject;
        configureProcessor();
    }
    
    private void configureProcessor(){
        if(this.processorObject!=null){
            logger.info("Configuring processor: " + this.processorObject.getClass().getName());
            // Messaging system setup
            Class pClass = processorObject.getClass();
            Annotation[] annotations = pClass.getAnnotationsByType(MessageProcessor.class);
            if(annotations.length==1){
                MessageProcessor mpa = (MessageProcessor)annotations[0];
                serverName = resolve(mpa.configSource(), mpa.serverName());
                serverPort = Integer.parseInt(resolve(mpa.configSource(), Integer.toString(mpa.port())));
                logger.info("Configured messaging service: " + serverName + ":" + serverPort);
                        
            } else {
                serverName = "localhost";
                serverPort = 9902;
                logger.warning("Using defaults for messaging service");
            }
            
            // Find the processor message handler
            Method[] methods = pClass.getDeclaredMethods();
            for(Method m : methods){
                if(m.getAnnotation(HandleMessage.class)!=null){
                    logger.info("Found handler method: " + m.getName());                    
                    HandleMessage hma = (HandleMessage)m.getAnnotation(HandleMessage.class);
                    MessageHandler handler = new MessageHandler(this, processorObject, m, hma);
                    handlerMap.put(hma.inputName(), handler);
                }
            }
        }
    }
    
    /**
     * Connect queues and start to receive messages
     */
    public void start(){
        for(MessageHandler h : handlerMap.values()){
            new Thread(h).start();
        }
    }
    
    /**
     * Disconnect everything and stop messages
     */
    public void shutdown(){
        for(MessageHandler h : handlerMap.values()){
            h.shutdown();
        }
    }
    
    public static String resolve(final SourceType sourceType, final String variable) {
        if(sourceType==SourceType.ENVIRONMENT){
            String value = System.getProperty(variable);
            if (value == null) {
                // than we try ENV ...
                value = System.getenv(variable);
            }
            return value;
        } else {
            return variable;
        }
    }
    
    /** Create a Kafka consumer attached to a queue */
    public Consumer<Long, byte[]> createConsumer(String groupName, String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverName + ":" + serverPort);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Consumer<Long, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }
    
    /** Create a Kafka producer for a queue */
    public Producer<Long, byte[]> createProducer(String groupName) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverName + ":" + serverPort);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, groupName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        
        return new KafkaProducer<>(props);
    }    
   
}