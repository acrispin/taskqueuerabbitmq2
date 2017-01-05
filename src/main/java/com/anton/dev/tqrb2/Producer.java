/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Producer extends MessageQueueEndPoint2 {
    
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private static final String QUEUE_NAME = "MY_QUEUE";
    private static Producer instance;
    
    private Producer(String queueName) throws TimeoutException {
        super(queueName);
    }
    
    public static Producer getInstance() {
        if (instance == null) {
            synchronized (Producer.class) {
                if(instance == null){
                    try {
                        instance = new Producer(QUEUE_NAME);
                        LOGGER.info("Instancia de Producer creada con la cola: " + QUEUE_NAME + " from thread " + Thread.currentThread().getName());
                    } catch (TimeoutException ex) {
                        LOGGER.error("Error en crear la instancia de Producer en la cola: " + QUEUE_NAME);
                        LOGGER.error("TimeoutException: " + ex.getMessage());
                    }
                }
            }
        }
        return instance;
    }
    
    public void publishMessage(HashMap<String, Object> msgMap) {
        try {
            this.getChannel().basicPublish("", endPointName, null, SerializationUtils.serialize(msgMap));
        } catch (IOException ex) {
            LOGGER.error("Error en publicar mensaje en la cola: " + endPointName);
            LOGGER.error("IOException: " + ex.getMessage());
        }
    }
}