package com.shaturko.practice.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();) {

            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter severity >>");
            String severity = sc.nextLine();
            System.out.println("Enter message >>");
            String message = sc.nextLine();

            channel.basicPublish(EXCHANGE_NAME, severity, null
                    , message.getBytes(StandardCharsets.UTF_8));

            System.out.println("Sent '" + message + "'");
        }
    }
}
