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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

import static op.kompetensdag.snake.Topics.GAME_MOVEMENT_COMMANDS_TOPIC;
import static op.kompetensdag.snake.model.HeadDirection.*;

@Component
public class MovementProcessor {

    private static StreamsBuilder streamsBuilder;

    private static SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde;
    private static SpecificAvroSerde<HeadDirectionRecord> headDirSerde;

    private static KTable<String, GameStatusRecord> gameStatusKTable;
    private static KTable<String, HeadDirectionRecord> headDirectionRecordKTable3;

    public MovementProcessor(final StreamsBuilder streamsBuilder,
                             final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde,
                             final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                             final KTable<String, GameStatusRecord> gameStatusKTable,
                             final KTable<String, HeadDirectionRecord> headDirectionRecordKTable3) {
        MovementProcessor.streamsBuilder = streamsBuilder;
        MovementProcessor.gameMovementKeyPressedSerde = gameMovementKeyPressedSerde;
        MovementProcessor.headDirSerde = headDirSerde;
        MovementProcessor.gameStatusKTable = gameStatusKTable;
        MovementProcessor.headDirectionRecordKTable3 = headDirectionRecordKTable3;
    }

    public static void define() {
        streamsBuilder
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
