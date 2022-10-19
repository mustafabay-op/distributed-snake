package op.kompetensdag.snake;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;



public class SnakeApplication {

    private static final String GAME_INPUT = "game-input";
    private static final String GAME_MOVEMENT_COMMANDS = "movement-commands";

    private static final String GAME_ADMINISTRATION_COMMANDS = "game-admin-commands";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> gameInput = builder.stream(GAME_INPUT,
                Consumed.with(Serdes.String(), Serdes.String()));

        BranchedKStream<String, String> gameInputBranched =
                builder.stream(GAME_INPUT, Consumed.with(Serdes.String(), Serdes.String())).split();

        gameInputBranched.branch(
                (k, v) -> GameMovementKeyPressed.valueOf(v) != null,
                Branched.withConsumer(stream -> stream.mapValues((k,v) -> GameMovementKeyPressed.valueOf(v))
                        .to(GAME_MOVEMENT_COMMANDS, Produced.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedValueSerde))));

        gameInputBranched.branch(
                (k, v) -> GameAdministrationKeyPressed.valueOf(v) != null,
                Branched.withConsumer(stream -> stream.mapValues((k,v) -> GameAdministrationKeyPressed.valueOf(v))
                .to(GAME_ADMINISTRATION_COMMANDS, Produced.with(Serdes.String(), CustomSerdes.gameAdministrationKeyPressedSerde))));

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(
                        v -> ""
                ).to("RESPONSES_STREAM", Produced.with(Serdes.String(), Serdes.String()))));










        gameInput.mapValues((k, v) -> GameMovementKeyPressed.valueOf(v)).to("Topic");

        KStream<String, GameMovementKeyPressed> commandsKStream3 = builder.stream("Topic");

        commandsKStream3.peek((k, v) -> System.out.println("Key: " + k + " Value: " + v));

        KTable<String, String> kTable = builder.table("topic-123");

        builder.stream("INVENTORY_COMMAND_STREAM",
                Consumed.with(Serdes.String(), Serdes.String()))
                .join(kTable, (inventoryCommand, locationData) -> {
                    return "2";
                }).to("topic-123");
/*        stema.initalizewithDirection

                GameStartedEvent skapar intialize state


        KStream<String, String> userLocationChanges =
                userPositionTable
                        .toStream()
                        .leftJoin(locationDataTable, (uuid, position) -> position,
                                (position, locationData) -> new LocationData(position.getX(), position.getY(),
                                        locationData == null ? null : locationData.getDESCRIPTION(),
                                        locationData == null ? null : locationData.getOBJECTS()));
*/


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