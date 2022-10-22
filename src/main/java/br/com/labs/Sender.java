package br.com.labs;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;

@Component
public class Sender {
    
    @Value("${spring.rabbitmq.host}")
    String host;
    
    @Value("${spring.rabbitmq.port}")
    Integer port;
    
    @Value("${spring.rabbitmq.username}")
    String username;
    
    @Value("${spring.rabbitmq.password}")
    String password;

    static void log(String format, Object... arguments) {
        System.out.println(String.format(format, arguments));
    }

    public void send() throws InterruptedException {
        log("Connecting...");
       // try (Environment environment =
       //  Environment.builder().host(host).username(username).password(password).build()){
        // try (Environment environment =
        // Environment.builder().host("localhost").username("rabbit_admin").password(".123-321.").build())
        // {
         //Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {
        Address entryPoint = new Address(host, port);
        System.out.println(host);
        System.out.println(port);
        try (Environment environment = Environment.builder().host(entryPoint.host()).port(entryPoint.port())
                .username(username).password(password).addressResolver(address -> entryPoint)
                .build()) {

            log("Connected");

            log("Creating stream...");
            environment.streamCreator().stream("finance.eletronics").create();
            log("Stream created");

            log("Creating producer...");
            Producer producer = environment.producerBuilder().stream("finance.eletronics").build();
            log("Producer created");

            long start = System.currentTimeMillis();
            int messageCount = 3;
            CountDownLatch confirmLatch = new CountDownLatch(messageCount);
            log("Sending %,d messages", messageCount);
            IntStream.range(0, messageCount).forEach(i -> {
                Message message = producer.messageBuilder().properties().creationTime(System.currentTimeMillis())
                        .messageId(i).messageBuilder().addData("hello world".getBytes(StandardCharsets.UTF_8)).build();
                producer.send(message, confirmationStatus -> confirmLatch.countDown());
            });
            log("Messages sent, waiting for confirmation...");
            boolean done = confirmLatch.await(1, TimeUnit.MINUTES);
            log("All messages confirmed? %s (%d ms)", done ? "yes" : "no", (System.currentTimeMillis() - start));
            log("Closing environment...");
        }
        log("Environment closed");
    }
}
