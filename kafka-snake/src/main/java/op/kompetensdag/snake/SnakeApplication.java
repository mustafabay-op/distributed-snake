package op.kompetensdag.snake;

import op.kompetensdag.snake.processors.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@Component
public class SnakeApplication {


    private static StreamsBuilder streamsBuilder;
    private static Properties streamProperties;

    @Autowired
    public SnakeApplication(final StreamsBuilder streamsBuilder,
                            final Properties streamProperties) {
        SnakeApplication.streamsBuilder = streamsBuilder;
        SnakeApplication.streamProperties = streamProperties;
    }

    public static void main(String[] args) {
        SpringApplication.run(SnakeApplication.class, args);


        GameInputRouter.define();
        AdministrationProcessor.define();
        MovementProcessor.define();
        TickGenerator.define();
        TickProcessor.define();
        GameUpdatesProcessor.define();


        final Topology topology = streamsBuilder.build();
        final KafkaStreams streams = new KafkaStreams(topology, streamProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}