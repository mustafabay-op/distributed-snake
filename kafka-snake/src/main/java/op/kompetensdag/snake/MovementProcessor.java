package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.*;
import op.kompetensdag.snake.util.Join;
import op.kompetensdag.snake.model.HeadDirection;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.net.ServerSocket;
import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class MovementProcessor {


    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable) {

        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        KTable<String, HeadDirectionRecord> headDirectionRecordKTable2 = builder.table(HEAD_DIRECTION_TOPIC_3, Consumed.with(Serdes.String(), headDirSerde));
        headDirectionRecordKTable2.mapValues(v -> "HeadDir: " + v + ", Dir: " + v.getType()).toStream().to(GAME_OUTPUT);

        builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .peek((key, value) -> System.out.println("Before join movementKey with headDirection, key: " + key + " value: " + value))
                .join(gameStatusKTable, (movementCommand, gameStatus) -> new Join<>(movementCommand, gameStatus))
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getType().equals(GameStatus.RUNNING))
                .mapValues(movementAndStateJoin -> movementAndStateJoin.l())
                .join(headDirectionRecordKTable2, (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection)) // Join::new) // , Consumed.with(Serdes.String(), headDirSerde)), (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection))
                .mapValues(movementAndHeadDirectionJoin -> new Join<>(getIntendedHeadDirection(movementAndHeadDirectionJoin.l().getType()), movementAndHeadDirectionJoin.r().getType()))
                .filter((k, intendedAndCurrentHeadDirection) -> isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(join -> new HeadDirectionRecord(join.l()))
                .to("head_direction_3", Produced.with(Serdes.String(), headDirSerde));

    }

    private static boolean isIntendedMoveValid(Join<HeadDirection, HeadDirection> intendedAndCurrentHeadDirection) {
        return (intendedAndCurrentHeadDirection.l() != intendedAndCurrentHeadDirection.r()) && (!isOpposite(intendedAndCurrentHeadDirection.l(), intendedAndCurrentHeadDirection.r()));
    }

    private static boolean isOpposite(HeadDirection newDirection, HeadDirection currentDirection) {
        return switch (currentDirection) {
            case NORTH -> newDirection.equals(HeadDirection.SOUTH);
            case SOUTH -> newDirection.equals(HeadDirection.NORTH);
            case EAST -> newDirection.equals(HeadDirection.WEST);
            case WEST -> newDirection.equals(HeadDirection.EAST);
            default -> true;
        };
    }

    private static HeadDirection getIntendedHeadDirection(GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> HeadDirection.NORTH;
            case DOWN -> HeadDirection.SOUTH;
            case RIGHT -> HeadDirection.EAST;
            case LEFT -> HeadDirection.WEST;
            default -> null;
        };
    }
}
