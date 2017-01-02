/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        // consumerMessages();
        // producerMessages(100);
        //testQueueConcurrent2(15);
        testQueue2(7);
        // testQueueConcurrent(1);
    }
    
    public static Consumer consumerMessages() {        
        //Spawn Consumer Thread, which will always listening for the messages to be processed
        Consumer consumer = Consumer.getInstance();
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        return consumer;
    }
    
    public static Producer producerMessages(int numMessage) {
        //Publishes msg in the queue
        Producer producer = Producer.getInstance();
        HashMap message = new HashMap();
        for (int i = 0; i < numMessage; i++) {
            // sleepMain(1000);
            LOGGER.info("Message #" + i + " sent to Queue.");
            message.put("My Message", i);
            producer.publishMessage(message);
        }
        return producer;
    }

    public static void testQueue2(int numMessage) {
        Consumer consumer = consumerMessages();
        Producer producer = producerMessages(numMessage);
        try {
            consumer.close();
        } catch (IOException ex) {
            LOGGER.error("IOException Consumer: " + ex.getMessage());
        }
        try {
            producer.close();
        } catch (IOException ex) {
            LOGGER.error("IOException Producer: " + ex.getMessage());
        }
    }

    public static void testQueueConcurrent2(int numThreads) throws InterruptedException {
        Consumer consumer = consumerMessages();
        List<Thread> listThreads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    producerMessages(1);
                }
            });
            listThreads.add(t1);
            t1.start();
        }

        for (Thread t : listThreads) {
            t.join();
        }
    }

    public static void testQueue(int numMessage) {
        // LOGGER.info("testQueue INI");
        //Spawn Consumer Thread, which will always listening for the messages to be processed
        Consumer consumer = Consumer.getInstance();
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        //Publishes msg in the queue
        Producer producer = Producer.getInstance();

        HashMap message = new HashMap();

        //Produce 10 msgs
        for (int i = 0; i < numMessage; i++) {
            // sleepMain(1000);
            LOGGER.info("Message #" + i + " sent to Queue.");
            message.put("My Message", i);
            producer.publishMessage(message);
        }
        // LOGGER.info("testQueue FIN");
    }

    public static void testQueueConcurrent(int numThreads) throws InterruptedException {
        List<Thread> listThreads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    testQueue(10);
                }
            });
            listThreads.add(t1);
            t1.start();
        }

        for (Thread t : listThreads) {
            t.join();
        }
    }
    
    public static void sleepMain(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            LOGGER.error("InterruptedException: " + ex.getMessage());
        }
    }
}
