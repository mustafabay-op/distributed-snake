package op.kompetensdag.snake;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;



public class SnakeApplication {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");

        Map<String, String> schemaRegistryProps = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));

        final StreamsBuilder builder = new StreamsBuilder();


        SpecificAvroSerde<GameMovementCommand> gameMovementCommandSpecificAvroSerde = new SpecificAvroSerde<>();
        gameMovementCommandSpecificAvroSerde.configure(schemaRegistryProps, false);

/*        builder.stream(GAME_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(GameMovementKeyPressedAvro::new)
                .to(GAME_COMMANDS, Produced.with(Serdes.String(), gameMovementKeyPressedAvroSpecificAvroSerde));

        builder.stream(GAME_COMMANDS, Consumed.with(Serdes.String(), gameMovementKeyPressedAvroSpecificAvroSerde))
                .mapValues(v -> v.KEYPRESSED + " : " + v.getKEYPRESSED() + "processed " + v.getClass())
                .to(GAME_OUTPUT);*/




        GameInputRouter.define(builder, schemaRegistryProps);
        // MovementProcessor.define(builder);




        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
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