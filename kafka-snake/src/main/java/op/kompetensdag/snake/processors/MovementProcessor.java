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

public class MovementProcessor {

    public static boolean isIntendedMoveValid(Join<HeadDirection, HeadDirection> intendedAndCurrentHeadDirection) {
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

    public static HeadDirection getIntendedHeadDirection(GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> NORTH;
            case DOWN -> SOUTH;
            case RIGHT -> EAST;
            case LEFT -> WEST;
            default -> null;
        };
    }
}
