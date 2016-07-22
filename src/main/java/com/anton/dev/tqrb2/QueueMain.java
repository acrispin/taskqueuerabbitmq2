/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author anton
 * http://www.javabydefault.com/2014/02/how-to-publish-and-consume-messages.html
 */
public class QueueMain {
    
    private static final Logger LOGGER = LogManager.getLogger(QueueMain.class);
    
    public static void main(String[] argv) throws Exception {
    
        final String QUEUE_NAME = "MY_QUEUE";
    
        //Spawn Consumer Thread, which will always listening for the messages to be processed
        Consumer consumer = new Consumer(QUEUE_NAME);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
    
        //Publishes msg in the queue
        Producer producer = new Producer(QUEUE_NAME);
        
        HashMap message = new HashMap();
        
        //Produce 100 msgs
        for (int i = 0; i < 100; i++) {
           // Thread.sleep(1000);
           LOGGER.info("Message #"+ i +" sent to Queue.");
           message.put("My Message", i);
           producer.publishMessage(message);
        }
    }
}
