package com.shaturko.practice.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCClient {

    private Connection connection;
    private Channel channel;
    private final String requestQueue = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        RPCClient client = new RPCClient();
        for(int i = 0; i < 32; i++) {
            String message = Integer.valueOf(i).toString();
            String result = client.call(message);
            System.out.println("Fibo for " + i + " = " + result);
        }

    }

    public String call(String message) throws IOException, ExecutionException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        channel.queueDeclare(requestQueue, false, false, false, null);
        String replyQueue = channel.queueDeclare().getQueue();

        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueue)
                .build();

        channel.basicPublish("", requestQueue, basicProperties, message.getBytes(StandardCharsets.UTF_8));

        CompletableFuture<String> response = new CompletableFuture<>();

        DeliverCallback callback = (consumerTag, delivery) -> {
            if(delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.complete(new String(delivery.getBody(), StandardCharsets.UTF_8));
            }

        };

        String cTag = channel.basicConsume("replyQueue", true, callback, consumerTag ->{});
        String result = response.get();

        channel.basicCancel(cTag);

        return null;
    }
}
