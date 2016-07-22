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

public class Consumer extends MessageQueueEndPoint implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);

    public Consumer(String queueName) throws TimeoutException {
        super(queueName);
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
