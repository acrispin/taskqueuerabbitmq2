/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author DSB Mobile ASUS AE15
 */
public class ConsumerListener implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ConsumerListener.class);

    @Override
    public void run() {
        try {
            Consumer2 consumer2 = Consumer2.getInstance();
            QueueingConsumer consumer = new QueueingConsumer(consumer2.getChannel());
            //start consuming messages. Auto acknowledge messages.
//            consumer2.getChannel().basicConsume(consumer2.getEndPointName(), true, consumer);
            
            while (true) { //keep listening for the message
                try {
//                     HashMap<String, Object> msgMap = consumeMessage(consumer);
                    HashMap<String, Object> msgMap = consumeMessage(consumer2.getChannel(), consumer2.getEndPointName());
                    if (msgMap != null) {
                        for (String key : msgMap.keySet()) {
                            LOGGER.info("Message " + key + "-" + msgMap.get(key) + " received from Queue from thread " + Thread.currentThread().getName());
                        }
                        Thread.sleep(100);
                    } else {
                        Thread.sleep(1000);
                    }
                    
                } catch (IOException | InterruptedException | SerializationException ex) {
                    LOGGER.error("Error received from Queue", ex);
                }                
            }
        } catch (IOException | ShutdownSignalException ex) {
            LOGGER.error("Error connecting to Queue: ", ex.getMessage());
            LOGGER.error(ex);
        }
    }
    
    

    /**
     * Blocking method, return only when something is available from the Queue
     *
     * @param consumer
     * @return
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    private HashMap<String, Object> consumeMessage(QueueingConsumer consumer) throws IOException, InterruptedException {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery(); //blocking call
        return (HashMap<String, Object>) SerializationUtils.deserialize(delivery.getBody());
    }
    
    /**
     * Blocking method, return only when something is available from the Queue
     *
     * @param consumer
     * @return
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    private HashMap<String, Object> consumeMessage(Channel channel, String queueName) throws IOException, InterruptedException {
        HashMap<String, Object> result = null;
        GetResponse response = channel.basicGet(queueName, true);
        if (response != null) {
            result = (HashMap<String, Object>) SerializationUtils.deserialize(response.getBody());
        }        
        return result;
    }
}
