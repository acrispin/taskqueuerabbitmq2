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

public final class Consumer extends MessageQueueEndPoint implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);   
    private static Consumer instance;

    private Consumer(String queueName) throws TimeoutException {
        super(queueName);
    }
    
    public static Consumer getInstance(String queueName) {
        if (instance == null) {
            synchronized (Producer.class) {
                if(instance == null){
                    try {
                        instance = new Consumer(queueName);
                        LOGGER.info("Instancia de Consumer creada con la cola: " + queueName);
                    } catch (TimeoutException ex) {
                        LOGGER.error("Error en crear la instancia de Consumer en la cola: " + queueName);
                        LOGGER.error("TimeoutException: " + ex.getMessage());
                    }
                }
            }
        }
        return instance;
    }

    @Override
    public void run() {        
        try {
            QueueingConsumer consumer = new QueueingConsumer(channel);
            //start consuming messages. Auto acknowledge messages.
            channel.basicConsume(endPointName, true, consumer);
            while (true) { //keep listening for the message
                HashMap<String, Integer> msgMap = consumeMessage(consumer);
                LOGGER.info("Message #" + msgMap.get("My Message") + " received from Queue.");
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Error connecting to Queue.", e);
        }        
    }

    /**
     * Blocking method, return only when something is available from the Queue
     *
     * @return
     * @throws Exception
     */
    private HashMap<String, Integer> consumeMessage(QueueingConsumer consumer) throws IOException, InterruptedException {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery(); //blocking call
        return (HashMap<String, Integer>) SerializationUtils.deserialize(delivery.getBody());
    }
}
