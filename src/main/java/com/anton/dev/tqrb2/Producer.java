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

public class Producer extends MessageQueueEndPoint {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    
    public Producer(String queueName) throws TimeoutException {
        super(queueName);
    }
    
    public void publishMessage(HashMap<String, Integer> msgMap) {
        try {
            channel.basicPublish("", endPointName, null, SerializationUtils.serialize(msgMap));
        } catch (IOException e) {
            LOGGER.error("Error connecting to Queue." , e);
        }
    }
}