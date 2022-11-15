package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.GAME_MOVEMENT_COMMANDS_TOPIC;
import static op.kompetensdag.snake.model.HeadDirection.*;

@Builder
public class MovementProcessor {

    private GameMovementKeyPressedRecord gameMovement;
    private GameStatusRecord gameStatus;
    private HeadDirectionRecord intendedHeadDirection;
    private HeadDirectionRecord currentHeadDirection;

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable, KTable<String, HeadDirectionRecord> currentHeadDirectionTable) {

        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();

        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);
        headDirSerde.configure(schemaRegistryProps, false);

        builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .mapValues((game, movement) -> MovementProcessor.builder().intendedHeadDirection(getIntendedHeadDirection(movement.getType())))
                .join(gameStatusKTable, MovementProcessorBuilder::gameStatus)
                .filter((k, cmdBuilder) -> cmdBuilder.gameStatus.equals(new GameStatusRecord(GameStatus.RUNNING)))
                .join(currentHeadDirectionTable, MovementProcessorBuilder::currentHeadDirection)
                .filter((k, cmdBuilder) -> cmdBuilder.build().isIntendedMoveValid())
                .mapValues(cmdBuilder -> new HeadDirectionRecord(cmdBuilder.build().intendedHeadDirection.getType()))
                .to(Topics.HEAD_DIRECTION_TOPIC_3, Produced.with(Serdes.String(), headDirSerde));

        /*currentHeadDirectionTable.mapValues(v -> "HeadDir: " + v).toStream().to(GAME_OUTPUT);*/
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

    private static HeadDirectionRecord getIntendedHeadDirection(GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> new HeadDirectionRecord(NORTH);
            case DOWN -> new HeadDirectionRecord(SOUTH);
            case RIGHT -> new HeadDirectionRecord(EAST);
            case LEFT -> new HeadDirectionRecord(WEST);
            default -> null;
        };
    }

    public boolean isIntendedMoveValid() {
        return intendedHeadDirection != currentHeadDirection && !isOpposite(intendedHeadDirection.getType(), currentHeadDirection.getType());
    }
}
