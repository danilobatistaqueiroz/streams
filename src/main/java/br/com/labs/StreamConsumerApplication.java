package br.com.labs;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;

@SpringBootApplication
//@PropertySource("classpath:sender.properties")
public class StreamConsumerApplication {

    static void log(String format, Object... arguments) {
        System.out.println(String.format(format, arguments));
    }

    public static class Publish {

        public static void main(String[] args) throws Exception {
            ApplicationContext applicationContext = SpringApplication.run(StreamConsumerApplication.class, args);
            Sender sender= applicationContext.getBean("sender", Sender.class);
            sender.send();
        }
    }

    public static class Consume {

        public static void main(String[] args) throws Exception {
            log("Connecting...");
            Address entryPoint = new Address("localhost", 5555);
            System.out.println(entryPoint.host());
            System.out.println(entryPoint.port());
            try (Environment environment = Environment.builder().host(entryPoint.host()).port(entryPoint.port())
                    .username("rabbit_admin").password(".123-321.").addressResolver(address -> entryPoint).build()) {
                
            //try (Environment environment =
            //        Environment.builder().host("localhost").port(5555).username("rabbit_admin").password(".123-321.").build()){

                log("Connected");

                AtomicInteger messageConsumed = new AtomicInteger(0);
                long start = System.currentTimeMillis();
                log("Start consumer...");
                
                System.out.println(environment.toString());

                Consumer consumer = environment.consumerBuilder().stream("finance.eletronics")
                        //.offset(OffsetSpecification.first())
                        .offset(OffsetSpecification.offset(0))
                        //.offset(OffsetSpecification.timestamp(start-1_000_000_000L))
                        //.offset(OffsetSpecification.timestamp(start-1_000_000))
                        .messageHandler((context, message) -> {
                            messageConsumed.incrementAndGet();
                            System.out.println("Received: "+new String(message.getBodyAsBinary()));
                        })
                        .build();
                
                System.out.println("consumer built");
                
                Utils.waitAtMost(60, () -> messageConsumed.get() >= 1_000_000);
                log("Consumed %,d messages in %s ms", messageConsumed.get(), (System.currentTimeMillis() - start));
                log("Closing environment...");
            }
            log("Environment closed");
        }
    }
}
