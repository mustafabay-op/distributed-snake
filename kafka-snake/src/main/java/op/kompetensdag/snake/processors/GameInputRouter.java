package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.GameAdministrationCommand;
import op.kompetensdag.snake.model.GameAdministrationCommandRecord;
import op.kompetensdag.snake.model.GameMovementKeyPressed;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

public class GameInputRouter {


    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps) {

        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde = new SpecificAvroSerde<>();

        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);
        gameAdministrationSerde.configure(schemaRegistryProps, false);

        BranchedKStream<String, String> gameInputBranched = builder
                .stream(Topics.GAME_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
                .split();

        gameInputBranched.branch(isGameMovementKeyPressedEvent(),
                Branched.withConsumer(stream -> stream.mapValues(value -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.valueOf(value)))
                        .to(Topics.GAME_MOVEMENT_COMMANDS, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));

        gameInputBranched.branch(isGameAdministrationKeyPressedEvent(),
                Branched.withConsumer(stream -> stream.mapValues(value -> new GameAdministrationCommandRecord(GameAdministrationCommand.valueOf(value)))
                        .to(Topics.GAME_ADMINISTRATION_COMMANDS, Produced.with(Serdes.String(), gameAdministrationSerde))));

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(v -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.LEFT))
                        .to(Topics.ILLEGAL_ARGUMENTS, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));

/*        builder.stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .mapValues(v -> "GameMovement: " + v)
                .to(GAME_OUTPUT);

        builder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdministrationSerde))
                .mapValues(v -> "GameAdmin: " + v)
                .to(GAME_OUTPUT);*/
    }

    private static Predicate<String, String> isGameMovementKeyPressedEvent() {
        return (k, v) -> {
            try {
                GameMovementKeyPressed.valueOf(v);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    private static Predicate<String, String> isGameAdministrationKeyPressedEvent() {
        return (k, v) -> {
            try {
                GameAdministrationCommand.valueOf(v);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }
}
