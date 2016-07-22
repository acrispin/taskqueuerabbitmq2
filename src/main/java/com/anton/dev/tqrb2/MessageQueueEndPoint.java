/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anton.dev.tqrb2;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Creates a connection to the Queue
 * 
 */
public abstract class MessageQueueEndPoint {
    protected Connection connection;
    protected Channel channel;
    protected String endPointName;
 
    private static final Logger LOGGER = LogManager.getLogger(MessageQueueEndPoint.class);

    public MessageQueueEndPoint(String queueName) throws TimeoutException {
        this.endPointName = queueName;
  
        //Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();
        
        //Replace with the correct connection uri
        String uri = "amqp://guest:guest@localhost:5672";
        try {
            factory.setUri(uri);
        
            //getting a connection
            connection = factory.newConnection();
     
            //creating a channel
            channel = connection.createChannel();
      
            //declaring a queue for this channel. If queue does not exist, it will be created on the server.
            //durability (second param) is also set as TRUE (the queue will survive a server restart)
            channel.queueDeclare(queueName, true, false, false, null);
        } catch (IOException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException ex) {
            LOGGER.error("Error connecting to MQ Server.", ex);
        }
    }
 
 
    /**
     * Closes the Queue Connection. This is not needed to be called explicitly as connection closure happens implicitly anyways.
     * @throws IOException
     */
     public void close() throws IOException{
         this.connection.close(); //closing connection, closes all the open channels
     }
 
     public int getCurrentMessageCount() throws IOException {
         return channel.queueDeclarePassive(this.endPointName).getMessageCount();
     }
}