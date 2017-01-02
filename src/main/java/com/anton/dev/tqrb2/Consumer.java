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
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.SerializationException;

public final class Consumer extends MessageQueueEndPoint2 implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private static final String QUEUE_NAME = "MY_QUEUE";
    private static Consumer instance;

    private Consumer(String queueName) throws TimeoutException {
        super(queueName);
    }
    
    public static Consumer getInstance() {
        if (instance == null) {
            synchronized (Consumer.class) {
                if(instance == null){
                    try {
                        instance = new Consumer(QUEUE_NAME);
                        LOGGER.info("Instancia de Consumer creada con la cola: " + QUEUE_NAME);
                    } catch (TimeoutException ex) {
                        LOGGER.error("Error en crear la instancia de Consumer en la cola: " + QUEUE_NAME);
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
            QueueingConsumer consumer = new QueueingConsumer(this.getChannel());
            //start consuming messages. Auto acknowledge messages.
            this.getChannel().basicConsume(endPointName, true, consumer);
            
            while (true) { //keep listening for the message
                try {
                    HashMap<String, Integer> msgMap = consumeMessage(consumer);
                    LOGGER.info("Message #" + msgMap.get("My Message") + " received from Queue.");
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
     * @return
     * @throws Exception
     */
    private HashMap<String, Integer> consumeMessage(QueueingConsumer consumer) throws IOException, InterruptedException {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery(); //blocking call
        return (HashMap<String, Integer>) SerializationUtils.deserialize(delivery.getBody());
    }
}
