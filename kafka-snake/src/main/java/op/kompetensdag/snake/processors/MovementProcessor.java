package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.*;
import op.kompetensdag.snake.util.Join;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;
import static op.kompetensdag.snake.model.HeadDirection.*;

public class MovementProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable, KTable<String, HeadDirectionRecord> headDirectionRecordKTable3) {

        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

/*

        headDirectionRecordKTable3.mapValues(v -> "HeadDir: " + v).toStream().to(GAME_OUTPUT);
*/

        builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .join(gameStatusKTable, (movementCommand, gameStatus) -> new Join<>(movementCommand, gameStatus))
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getType().equals(GameStatus.RUNNING))
                .mapValues(movementAndStateJoin -> movementAndStateJoin.l())
                .join(headDirectionRecordKTable3, (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection))
                .mapValues(movementAndHeadDirectionJoin -> new Join<>(getIntendedHeadDirection(movementAndHeadDirectionJoin.l().getType()), movementAndHeadDirectionJoin.r().getType()))
                .filter((k, intendedAndCurrentHeadDirection) -> isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(join -> new HeadDirectionRecord(join.l()))
                .to(Topics.HEAD_DIRECTION_TOPIC_3, Produced.with(Serdes.String(), headDirSerde));
    }

    private static boolean isIntendedMoveValid(Join<HeadDirection, HeadDirection> intendedAndCurrentHeadDirection) {
        return (intendedAndCurrentHeadDirection.l() != intendedAndCurrentHeadDirection.r()) && (!isOpposite(intendedAndCurrentHeadDirection.l(), intendedAndCurrentHeadDirection.r()));
    }

    private static boolean isOpposite(HeadDirection newDirection, HeadDirection currentDirection) {
        return switch (currentDirection) {
            case NORTH -> newDirection.equals(SOUTH);
            case SOUTH -> newDirection.equals(NORTH);
            case EAST -> newDirection.equals(WEST);
            case WEST -> newDirection.equals(EAST);
            default -> true;
        };
    }

    private static HeadDirection getIntendedHeadDirection(GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> NORTH;
            case DOWN -> SOUTH;
            case RIGHT -> EAST;
            case LEFT -> WEST;
            default -> null;
        };
    }
}
