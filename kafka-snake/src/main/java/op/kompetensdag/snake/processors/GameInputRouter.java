package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameAdministrationCommand;
import op.kompetensdag.snake.model.GameAdministrationCommandRecord;
import op.kompetensdag.snake.model.GameMovementKeyPressed;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

import static op.kompetensdag.snake.Topics.*;

@Component
public class GameInputRouter {

    private static BranchedKStream<String, String> gameInputBranched;
    private static SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde;
    private static SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde;

    @Autowired
    public GameInputRouter(final BranchedKStream<String, String> gameInputBranched,
                           final SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde,
                           final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde) {
        GameInputRouter.gameInputBranched = gameInputBranched;
        GameInputRouter.gameAdministrationSerde = gameAdministrationSerde;
        GameInputRouter.gameMovementKeyPressedSerde = gameMovementKeyPressedSerde;
    }

    public static void define() {
        gameInputBranched.branch(isGameMovementKeyPressedEvent(),
                Branched.withConsumer(stream -> stream.mapValues(value -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.valueOf(value)))
                        .to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));

        gameInputBranched.branch(isGameAdministrationKeyPressedEvent(),
                Branched.withConsumer(stream -> stream.mapValues(value -> new GameAdministrationCommandRecord(GameAdministrationCommand.valueOf(value)))
                        .to(GAME_ADMINISTRATION_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameAdministrationSerde))));

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(v -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.LEFT))
                        .to(ILLEGAL_ARGUMENTS_TOPIC, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));
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
