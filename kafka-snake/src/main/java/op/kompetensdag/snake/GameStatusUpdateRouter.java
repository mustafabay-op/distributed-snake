/*
package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.events.GameStatusUpdated;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class GameStatusUpdateRouter {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps) {

        SpecificAvroSerde<GameStatus> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);


        BranchedKStream<String, GameStatus> gameStatusUpdated = builder
                .stream(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde))
                .split();

        gameStatusUpdated.branch(
                (k, v) -> v.getSTATUS().equals(new GameStatus(GameStatusUpdated.INITIALIZING.name())),
                Branched.withConsumer(stream ->
                        stream
                        .mapValues(value -> value)
                        .to(GAME_INITIALIZING_TOPIC,
                        Produced.with(Serdes.String(), gameStatusSerde))));




        commandBranches.branch((k, v) -> {
            return DirectionParser.parse(v.getCOMMAND()) != null;
        }, Branched.withConsumer(stream -> stream.mapValues(v -> {
            return DirectionParser.parse(v.getCOMMAND());
        }).to(MOVEMENT_COMMAND_STREAM, Produced.with(Serdes.UUID(), movementCommandValueSerde))));


*/
/*

        gameInputBranched.branch(
                (k, v) -> GameMovementCommandParser.parse(v) != null,
                Branched.withConsumer(stream -> stream.mapValues(GameMovementCommandParser::parse)
                        .to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))));

        gameInputBranched.branch(
                (k, v) -> GameAdministrationCommandParser.parse(v) != null,
                Branched.withConsumer(stream -> stream.mapValues(GameAdministrationCommandParser::parse)
                        .to(GAME_ADMINISTRATION_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameAdministrationCommandSpecificAvroSerde))));

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(
                        v -> GameMovementCommandParser.parse("DOWN")
                ).to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))));

        builder.stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))
                .mapValues(v -> v.KEYPRESSED + " : " + v.getKEYPRESSED() + "processed " + v.getClass())
                .to(GAME_OUTPUT);

        builder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdministrationCommandSpecificAvroSerde))
                .mapValues(v -> v.KEYPRESSED + " : " + v.getKEYPRESSED() + "processed " + v.getClass())
                .to(GAME_OUTPUT);
*//*


    }
}
*/
