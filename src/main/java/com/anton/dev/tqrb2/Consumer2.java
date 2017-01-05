/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import java.util.HashMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public final class Consumer2 extends MessageQueueEndPoint2 {

    private static final Logger LOGGER = LogManager.getLogger(Consumer2.class);
    private static final String QUEUE_NAME = "MY_QUEUE";
    private static Consumer2 instance;

    private Consumer2(String queueName) throws TimeoutException {
        super(queueName);
    }
    
    public static Consumer2 getInstance() {
        if (instance == null) {
            synchronized (Consumer2.class) {
                if(instance == null){
                    try {
                        instance = new Consumer2(QUEUE_NAME);
                        LOGGER.info("Instancia de Consumer2 creada con la cola: " + QUEUE_NAME + " from thread " + Thread.currentThread().getName());
                    } catch (TimeoutException ex) {
                        LOGGER.error("Error en crear la instancia de Consumer en la cola: " + QUEUE_NAME);
                        LOGGER.error("TimeoutException: " + ex.getMessage());
                    }
                }
            }
        }
        return instance;
    }

    /**
     * Blocking method, return only when something is available from the Queue
     *
     * @param consumer
     * @return
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    public HashMap<String, Object> consumeMessage(QueueingConsumer consumer) throws IOException, InterruptedException {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery(); //blocking call
        return (HashMap<String, Object>) SerializationUtils.deserialize(delivery.getBody());
    }
}
