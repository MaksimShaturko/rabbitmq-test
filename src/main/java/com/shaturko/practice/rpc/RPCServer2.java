package com.shaturko.practice.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class RPCServer2 {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);


        DeliverCallback callback = (consumerTag, delivery) -> {

            AMQP.BasicProperties properties = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String res = Integer.toString(fib(Integer.parseInt(message)));

            channel.basicPublish("", delivery.getProperties().getReplyTo()
                    , properties, res.getBytes(StandardCharsets.UTF_8));
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, callback, consumerTag -> {
        });

    }


    private static int fib(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }

        return fib(n - 1) + fib(n - 2);
    }
}
